/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class NodeOrchestratingService extends Actor implements Service<NodeOrchestratingService>, TopologyMemberListener, TopologyPartitionListener
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;
    public static final Duration NODE_PENDING_TIMEOUT = Duration.ofMinutes(2);

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
                    nodeLoad.removePending(partitionInfo);
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

    public ActorFuture<NodeInfo> getNextSocketAddress(List<NodeInfo> except, PartitionInfo forPartitionInfo)
    {
        final CompletableActorFuture<NodeInfo> nextAddressFuture = new CompletableActorFuture<>();
        actor.run(() ->
        {
            final NodeLoad nextNode;
            if (except == null || except.isEmpty())
            {
                nextNode = loads.get(0);
            }
            else
            {
                final Optional<NodeLoad> nextOptional = loads.stream()
                    .filter(nodeLoad -> !except.contains(nodeLoad.getNodeInfo()))
                    .min(this::loadComparator);

                nextNode = nextOptional.isPresent() ? nextOptional.get() : null;
            }

            if (nextNode != null)
            {
                actor.runDelayed(NODE_PENDING_TIMEOUT, () -> nextNode.removePending(forPartitionInfo));
                nextNode.addPendingPartiton(forPartitionInfo);
                nextAddressFuture.complete(nextNode.getNodeInfo());
            }
            else
            {
                final String errorMessage = String.format("Found no next address, from current state %s with the excepted list %s", loads, except);
                nextAddressFuture.completeExceptionally(new IllegalStateException(errorMessage));
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
        final int comparedLoad = Integer.compare(load1.getLoad().size(), load2.getLoad().size());
        if (comparedLoad == 0)
        {
            return Integer.compare(load1.getPendings().size(), load2.getPendings().size());
        }
        return comparedLoad;
    }
}
