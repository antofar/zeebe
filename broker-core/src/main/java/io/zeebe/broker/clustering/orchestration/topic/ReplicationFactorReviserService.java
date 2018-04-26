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
package io.zeebe.broker.clustering.orchestration.topic;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.InvitationRequest;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.clustering.orchestration.NodeOrchestratingService;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.broker.clustering.orchestration.state.TopicInfo;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class ReplicationFactorReviserService extends Actor implements Service<ReplicationFactorReviserService>
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    public static final Duration TIMER_RATE = Duration.ofSeconds(1);
    public static final Duration PENDING_TOPIC_CREATION_TIMEOUT = Duration.ofMinutes(1);

    private final Injector<ClusterTopicState> stateInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
    private final Injector<NodeOrchestratingService> nodeOrchestratingServiceInjector = new Injector<>();
    private final Injector<ClientTransport> managementClientApiInjector = new Injector<>();

    private ClusterTopicState clusterTopicState;
    private TopologyManager topologyManager;
    private NodeOrchestratingService nodeOrchestratingService;
    private ClientTransport clientTransport;

    private Set<Integer> pendingInvitations;

    @Override
    public void start(final ServiceStartContext startContext)
    {
        clusterTopicState = stateInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();
        nodeOrchestratingService = nodeOrchestratingServiceInjector.getValue();
        clientTransport = managementClientApiInjector.getValue();

        pendingInvitations = new HashSet<>();

        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public void stop(final ServiceStopContext stopContext)
    {
        stopContext.async(actor.close());
    }

    @Override
    public String getName()
    {
        return "create-topic";
    }

    @Override
    protected void onActorStarted()
    {
        actor.runAtFixedRate(TIMER_RATE, this::replicationFactorRevising);
    }

    private void replicationFactorRevising()
    {
        final ActorFuture<Map<DirectBuffer, TopicInfo>> stateFuture = clusterTopicState.getDesiredState();

        actor.runOnCompletion(stateFuture, (desiredState, error) ->
        {
            if (error == null)
            {
                checkDesiredState(desiredState);
            }
            else
            {
                LOG.error("Unable to fetch expected cluster topic state", error);
            }
        });
    }

    private void checkDesiredState(final Map<DirectBuffer, TopicInfo> desiredState)
    {
        final ActorFuture<ClusterPartitionState> queryFuture = topologyManager.query(ClusterPartitionState::computeCurrentState);

        actor.runOnCompletion(queryFuture, (currentState, error) ->
        {
            if (error == null)
            {
                computeStateDifferences(desiredState, currentState);
            }
            else
            {
                LOG.error("Unable to compute current cluster topic state from topology", error);
            }
        });
    }

    private void computeStateDifferences(final Map<DirectBuffer, TopicInfo> desiredState,
                                         final ClusterPartitionState currentState)
    {

        for (final Map.Entry<DirectBuffer, TopicInfo> desiredEntry : desiredState.entrySet())
        {
            final TopicInfo desiredTopic = desiredEntry.getValue();
            final List<PartitionNodes> listOfPartitionNodes = currentState.getPartitions(desiredTopic.getTopicNameBuffer());
            for (final PartitionNodes partitionNode : listOfPartitionNodes)
            {
                final int missingReplications = partitionNode.getPartitionInfo().getReplicationFactor() - partitionNode.getNodes().size();
                if (missingReplications > 0)
                {
                    final int partitionId = partitionNode.getPartitionId();
                    if (!pendingInvitations.contains(partitionId))
                    {
                        LOG.debug("Inviting {} members for partition {}", missingReplications, partitionNode.getPartitionInfo());
                        for (int i = 0; i < missingReplications; i++)
                        {
                            inviteMember(partitionNode);
                        }
                        pendingInvitations.add(partitionId);
                        actor.runDelayed(PENDING_TOPIC_CREATION_TIMEOUT, () -> pendingInvitations.remove(partitionId));
                    }
                }
            }
        }
    }

    private void inviteMember(final PartitionNodes partitionNodes)
    {
        final ActorFuture<NodeInfo> nextSocketAddressFuture = nodeOrchestratingService.getNextSocketAddress(partitionNodes.getPartitionInfo());
        actor.runOnCompletion(nextSocketAddressFuture, (nodeInfo, error) ->
        {
            if (error == null)
            {
                LOG.debug("Send invite request for partition {} to node {}", partitionNodes.getPartitionInfo(), nodeInfo.getManagementApiAddress());
                sendInvitationRequest(partitionNodes, nodeInfo);
            }
            else
            {
                LOG.error("Problem in resolving next node address to invite for partition {}", partitionNodes.getPartitionInfo());
            }
        });

    }

    private void sendInvitationRequest(final PartitionNodes partitionNodes, final NodeInfo nodeInfo)
    {
        final PartitionInfo partitionInfo = partitionNodes.getPartitionInfo();
        final List<SocketAddress> members = partitionNodes.getNodes().stream()
                                                          .map(NodeInfo::getReplicationApiAddress)
                                                          .collect(Collectors.toList());

        final InvitationRequest request = new InvitationRequest()
            .topicName(partitionInfo.getTopicName())
            .partitionId(partitionInfo.getPartitionId())
            .replicationFactor(partitionInfo.getReplicationFactor())
            .members(members);

        final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(nodeInfo.getManagementApiAddress());
        final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

        actor.runOnCompletion(responseFuture, (createPartitionResponse, error) ->
        {
            if (error == null)
            {
                LOG.info("Member {} successfully invited to partition {}", nodeInfo.getManagementApiAddress(), partitionInfo);
            }
            else
            {
                LOG.warn("Failed to invite node {} to partition {}", nodeInfo.getManagementApiAddress(), partitionInfo, error);
            }
        });
    }

    @Override
    public ReplicationFactorReviserService get()
    {
        return this;
    }

    public Injector<ClusterTopicState> getStateInjector()
    {
        return stateInjector;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }

    public Injector<NodeOrchestratingService> getNodeOrchestratingServiceInjector()
    {
        return nodeOrchestratingServiceInjector;
    }

    public Injector<ClientTransport> getManagementClientApiInjector()
    {
        return managementClientApiInjector;
    }
}
