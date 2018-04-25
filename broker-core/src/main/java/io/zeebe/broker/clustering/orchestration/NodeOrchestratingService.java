package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.*;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class NodeOrchestratingService implements Service<NodeOrchestratingService>, TopologyMemberListener, TopologyPartitionListener
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
    }

    @Override
    public NodeOrchestratingService get()
    {
        return this;
    }

    @Override
    public void onMemberAdded(NodeInfo memberInfo, Topology topology)
    {
        LOG.debug("Add node {} to current state.", memberInfo);
        loads.add(new NodeLoad(memberInfo));
    }

    @Override
    public void onMemberRemoved(NodeInfo memberInfo, Topology topology)
    {
        LOG.debug("Remove node {} from current state.", memberInfo);
        loads.remove(new NodeLoad(memberInfo));
    }

    @Override
    public void onPartitionUpdated(PartitionInfo partitionInfo, NodeInfo member)
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
                LOG.debug("Increased load of node {} by partition {}", member, partitionInfo);
            }
        }
        else
        {
            LOG.debug("Node {} was not found in current state.", member);
        }
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }
}
