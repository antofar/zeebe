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
import java.util.*;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.CreatePartitionRequest;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.clustering.orchestration.NodeOrchestratingService;
import io.zeebe.broker.clustering.orchestration.id.IdGenerator;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.broker.clustering.orchestration.state.TopicInfo;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.transport.ClientResponse;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class TopicCreationReviserService extends Actor implements Service<TopicCreationReviserService>
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    public static final Duration TIMER_RATE = Duration.ofSeconds(1);
    public static final Duration PENDING_TOPIC_CREATION_TIMEOUT = Duration.ofMinutes(1);

    private final Injector<ClusterTopicState> stateInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
    private final Injector<Partition> leaderSystemPartitionInjector = new Injector<>();
    private final Injector<IdGenerator> idGeneratorInjector = new Injector<>();
    private final Injector<NodeOrchestratingService> nodeOrchestratingServiceInjector = new Injector<>();
    private final Injector<ClientTransport> managementClientApiInjector = new Injector<>();

    private ClusterTopicState clusterTopicState;
    private TopologyManager topologyManager;
    private TypedStreamReader streamReader;
    private TypedStreamWriter streamWriter;
    private IdGenerator idGenerator;
    private NodeOrchestratingService nodeOrchestratingService;
    private ClientTransport clientTransport;

    private Set<TopicInfo> pendingTopicCreation;

    @Override
    public void start(final ServiceStartContext startContext)
    {
        clusterTopicState = stateInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();
        idGenerator = idGeneratorInjector.getValue();
        nodeOrchestratingService = nodeOrchestratingServiceInjector.getValue();
        clientTransport = managementClientApiInjector.getValue();

        final Partition leaderSystemPartition = leaderSystemPartitionInjector.getValue();
        final TypedStreamEnvironment typedStreamEnvironment = new TypedStreamEnvironment(leaderSystemPartition.getLogStream(), null);
        streamReader = typedStreamEnvironment.buildStreamReader();
        streamWriter = typedStreamEnvironment.buildStreamWriter();

        pendingTopicCreation = new HashSet<>();

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
        actor.runAtFixedRate(TIMER_RATE, this::topicCreationRevising);
    }

    private void topicCreationRevising()
    {
        final ActorFuture<Map<DirectBuffer, TopicInfo>> stateFuture = clusterTopicState.getPendingTopics();

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
            final int missingPartitions = desiredTopic.getPartitionCount() - listOfPartitionNodes.size();
            if (missingPartitions > 0)
            {
                if (!pendingTopicCreation.contains(desiredTopic))
                {
                    LOG.debug("Creating {} partitions for topic {}", missingPartitions, desiredTopic.getTopicName());
                    for (int i = 0; i < missingPartitions; i++)
                    {
                        createPartition(desiredTopic);
                    }
                    pendingTopicCreation.add(desiredTopic);
                    actor.runDelayed(PENDING_TOPIC_CREATION_TIMEOUT, () -> pendingTopicCreation.remove(desiredTopic));
                }
            }
            else
            {
                LOG.debug("Requested partition count {} for topic {} reached", desiredTopic.getPartitionCount(), desiredTopic.getTopicName());

                final TypedEvent<TopicEvent> readEvent = streamReader.readValue(desiredTopic.getCreateEventPosition(), TopicEvent.class);
                final TopicEvent topicEvent = readEvent.getValue();
                listOfPartitionNodes.forEach(partitionNodes -> topicEvent.getPartitionIds().add().setValue(partitionNodes.getPartitionId()));
                topicEvent.setState(TopicState.CREATED);
                streamWriter.writeFollowupEvent(readEvent.getKey(), topicEvent);

                pendingTopicCreation.remove(desiredTopic);
            }
        }
    }

    private void createPartition(final TopicInfo topicInfo)
    {
        final ActorFuture<Integer> idFuture = idGenerator.nextId();
        actor.runOnCompletion(idFuture, (id, error) -> {
            if (error == null)
            {
                LOG.debug("Creating partition with id {} for topic {}", id, topicInfo.getTopicName());
                sendCreatePartitionRequest(topicInfo, id);
            }
            else
            {
                LOG.error("Failed to get new partition id for topic {}", topicInfo.getTopicName(), error);
            }
        });
    }

    private void sendCreatePartitionRequest(final TopicInfo topicInfo, final Integer partitionId)
    {
        final PartitionInfo newPartition = new PartitionInfo(topicInfo.getTopicNameBuffer(), partitionId, topicInfo.getReplicationFactor());
        final ActorFuture<NodeInfo> nextSocketAddressFuture = nodeOrchestratingService.getNextSocketAddress(newPartition);
        actor.runOnCompletion(nextSocketAddressFuture, (nodeInfo, error) ->
        {
            if (error == null)
            {
                LOG.debug("Send create partition request for topic {} to node {} with partition id {}", topicInfo.getTopicName(), nodeInfo.getManagementApiAddress(), partitionId);
                sendCreatePartitionRequest(topicInfo, partitionId, nodeInfo);
            }
            else
            {
                LOG.error("Problem in resolving next node address to create partition {} for topic {}", partitionId, topicInfo.getTopicName(), error);
            }
        });

    }

    private void sendCreatePartitionRequest(final TopicInfo topicInfo, final Integer partitionId, final NodeInfo nodeInfo)
    {
        final CreatePartitionRequest request = new CreatePartitionRequest()
            .topicName(topicInfo.getTopicNameBuffer())
            .partitionId(partitionId)
            .replicationFactor(topicInfo.getReplicationFactor());

        final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(nodeInfo.getManagementApiAddress());
        final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

        actor.runOnCompletion(responseFuture, (createPartitionResponse, error) ->
        {
            if (error == null)
            {
                LOG.info("Partition {} for topic {} created on node {}", partitionId, topicInfo.getTopicName(), nodeInfo.getManagementApiAddress());
            }
            else
            {
                LOG.warn("Failed to create partition {} for topic {} on node {}", partitionId, topicInfo.getTopicName(), nodeInfo.getManagementApiAddress(), error);
            }
        });
    }

    @Override
    public TopicCreationReviserService get()
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

    public Injector<Partition> getLeaderSystemPartitionInjector()
    {
        return leaderSystemPartitionInjector;
    }

    public Injector<IdGenerator> getIdGeneratorInjector()
    {
        return idGeneratorInjector;
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