package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.*;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class NodeOrchestratingService extends Actor implements Service<NodeOrchestratingService>, TopologyMemberListener, TopologyPartitionListener
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();

    private final List<NodeLoad> loads = new ArrayList<>();

    @Override
    public void start(ServiceStartContext startContext)
    {
        final TopologyManager topologyManager = topologyManagerInjector.getValue();
        topologyManager.addTopologyMemberListener(this);
        topologyManager.addTopologyPartitionListener(this);
        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public NodeOrchestratingService get()
    {
        return this;
    }

    @Override
    public void onMemberAdded(NodeInfo memberInfo, Topology topology)
    {
        actor.run(() ->
        {
            LOG.debug("Add node {} to current state.", memberInfo);
            loads.add(new NodeLoad(memberInfo));
            Collections.sort(loads, this::loadComparator);
        });
    }


    @Override
    public void onMemberRemoved(NodeInfo memberInfo, Topology topology)
    {
        actor.run(() ->
        {
            LOG.debug("Remove node {} from current state.", memberInfo);
            loads.remove(new NodeLoad(memberInfo));
            Collections.sort(loads, this::loadComparator);
        });
    }

    @Override
    public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member)
    {
        actor.run(() ->
        {
            final Optional<NodeLoad> nodeOptional = loads.stream()
                .filter(node -> node.getNodeInfo().equals(member))
                .findFirst();

            if (nodeOptional.isPresent())
            {
                final NodeLoad nodeLoad = nodeOptional.get();
                final boolean added = nodeLoad.addPartition(partitionInfo);
                if (added)
                {
                    Collections.sort(loads, this::loadComparator);
                    LOG.debug("Increased load of node {} by partition {}", member, partitionInfo);
                }
            }
            else
            {
                LOG.debug("Node {} was not found in current state.", member);
            }
        });
    }

    public ActorFuture<NodeInfo> getNextSocketAddress(List<NodeInfo> except)
    {
        final CompletableActorFuture<NodeInfo> nextAddressFuture = new CompletableActorFuture<>();
        actor.run(() ->
        {
            if (except == null || except.isEmpty())
            {
               nextAddressFuture.complete(loads.get(0).getNodeInfo()|);
            }
            else
            {
                final Optional<NodeLoad> nextOptional = loads.stream()
                    .filter(nodeLoad -> !except.contains(nodeLoad.getNodeInfo()))
                    .min(this::loadComparator);

                if (nextOptional.isPresent())
                {
                    nextAddressFuture.complete(nextOptional.get().getNodeInfo());
                }
                else
                {
                    final String errorMessage = String.format("Found no next address, from current state %s with the excepted list %s", loads, except);
                    nextAddressFuture.completeExceptionally(new IllegalStateException(errorMessage));
                }
            }
        });
        return nextAddressFuture;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }

    private int loadComparator(NodeLoad load1, NodeLoad load2)
    {
        return Integer.compare(load1.getLoad().size(), load2.getLoad().size());
    }
}
