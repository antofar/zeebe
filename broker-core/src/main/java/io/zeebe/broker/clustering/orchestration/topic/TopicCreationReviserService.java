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

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.CreatePartitionRequest;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
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
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

public class TopicCreationReviserService extends Actor implements Service<Void>
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    public static final Duration TIMER_RATE = Duration.ofSeconds(1);

    private final Injector<ClusterTopicState> stateInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();
    private final Injector<Partition> leaderSystemPartitionInjector = new Injector<>();
    private final Injector<IdGenerator> idGeneratorInjector = new Injector<>();
    private final Injector<NodeOrchestratingService> nodeOrchestratingServiceInjector = new Injector<>();
    private final Injector<ClientTransport> managmentClientApiInjector = new Injector<>();

    private ClusterTopicState clusterTopicState;
    private TopologyManager topologyManager;
    private Partition leaderSystemPartition;
    private TypedStreamReader streamReader;
    private TypedStreamWriter streamWriter;
    private IdGenerator idGenerator;
    private NodeOrchestratingService nodeOrchestratingService;
    private ClientTransport clientTransport;

    @Override
    public void start(final ServiceStartContext startContext)
    {
        clusterTopicState = stateInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();
        leaderSystemPartition = leaderSystemPartitionInjector.getValue();
        idGenerator = idGeneratorInjector.getValue();
        nodeOrchestratingService = nodeOrchestratingServiceInjector.getValue();
        clientTransport = managmentClientApiInjector.getValue();

        final TypedStreamEnvironment typedStreamEnvironment = new TypedStreamEnvironment(leaderSystemPartition.getLogStream(), null);
        streamReader = typedStreamEnvironment.buildStreamReader();
        streamWriter = typedStreamEnvironment.buildStreamWriter();

        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public void stop(final ServiceStopContext stopContext)
    {
        stopContext.async(actor.close());
    }


    @Override
    protected void onActorStarted()
    {
        actor.runAtFixedRate(TIMER_RATE, this::topicCreationRevising);
    }

    private void topicCreationRevising()
    {
        final ActorFuture<Map<DirectBuffer, TopicInfo>> stateFuture = clusterTopicState.getPendingTopics();

        actor.runOnCompletion(stateFuture, (desiredState, getDesiredStateError) ->
        {
            if (getDesiredStateError == null)
            {
                checkDesiredState(desiredState);
            }
            else
            {
                Loggers.ORCHESTRATION_LOGGER.error("Error in getting desired state.", getDesiredStateError);
            }
        });
    }

    private void checkDesiredState(final Map<DirectBuffer, TopicInfo> desiredState)
    {
        final ActorFuture<Map<DirectBuffer, List<PartitionInfo>>> queryFuture = topologyManager.query(this::computeCurrentState);

        actor.runOnCompletion(queryFuture, (currentState, readTopologyError) ->
        {
            if (readTopologyError == null)
            {
                computeStateDifferences(desiredState, currentState);
            }
            else
            {
                LOG.error("Error in reading topology.", readTopologyError);
            }
        });
    }

    private Map<DirectBuffer, List<PartitionInfo>> computeCurrentState(final ReadableTopology readableTopology)
    {
        final Map<DirectBuffer, List<PartitionInfo>> currentState = new HashMap<>();

        final Collection<PartitionInfo> partitions = readableTopology.getPartitions();

        partitions.forEach(partitionInfo ->
            currentState.compute(partitionInfo.getTopicName(),
                (s, partitionInfos) ->
                {
                    if (partitionInfos == null)
                    {
                        partitionInfos = new ArrayList<>();
                    }
                    partitionInfos.add(partitionInfo);
                    return partitionInfos;
                }));

        return currentState;
    }

    private void computeStateDifferences(final Map<DirectBuffer, TopicInfo> desiredState,
                                         final Map<DirectBuffer, List<PartitionInfo>> currentState)
    {
        for (final Map.Entry<DirectBuffer, TopicInfo> desiredEntry : desiredState.entrySet())
        {
            final TopicInfo desiredTopic = desiredEntry.getValue();

            final List<PartitionInfo> partitionInfos = currentState.get(desiredTopic.getTopicName());
            final int currentPartitionCount = partitionInfos != null ? partitionInfos.size() : 0;
            final int desiredPartitionCount = desiredTopic.getPartitionCount();
            final int missingPartitions = desiredPartitionCount - currentPartitionCount;
            if (missingPartitions > 0)
            {
                LOG.debug("Creating {} partitions for topic {}", missingPartitions, desiredTopic);
                for (int i = 0; i < missingPartitions; i++)
                {
                    createPartition(desiredTopic);
                }
                // TODO no partitions or not enough for this topic so we create partitions requests
                // we need Id generation for that
            }
            else
            {
                LOG.debug("Enough partitions created. Current state equals to desired state. Writing Topic {} CREATED.",
                    BufferUtil.bufferAsString(desiredTopic.getTopicName()));

                final TypedEvent<TopicEvent> readEvent = streamReader.readValue(desiredTopic.getCreateEventPosition(), TopicEvent.class);
                final TopicEvent topicEvent = readEvent.getValue();
                partitionInfos.forEach(info -> topicEvent.getPartitionIds().add().setValue(info.getPartitionId()));
                topicEvent.setState(TopicState.CREATED);
                streamWriter.writeFollowupEvent(readEvent.getKey(), topicEvent);
            }
        }
    }

    private void createPartition(final TopicInfo topicInfo)
    {
        final ActorFuture<Integer> idFuture = idGenerator.nextId();
        actor.runOnCompletion(idFuture, (id, throwable) -> {
            if (throwable == null)
            {
                LOG.debug("Creating partition with id {} for topic {}", id, topicInfo);
                sendCreatePartitionRequest(topicInfo, id);
            }
            else
            {
                LOG.error("Failed to get new partition for topic {}", topicInfo, throwable);
            }
        });
    }

    private void sendCreatePartitionRequest(final TopicInfo topicInfo, final Integer partitionId)
    {
        final ActorFuture<NodeInfo> nextSocketAddressFuture = nodeOrchestratingService.getNextSocketAddress(null);

        actor.runOnCompletion(nextSocketAddressFuture, (nodeInfo, throwable) ->
        {
            if (throwable == null)
            {
                LOG.debug("Got next node {} to send create partition request.", nodeInfo);


                final CreatePartitionRequest request = new CreatePartitionRequest()
                    .topicName(topicInfo.getTopicName())
                    .partitionId(partitionId)
                    .replicationFactor(topicInfo.getReplicationFactor());

                final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(nodeInfo.getManagementApiAddress());
                final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

                actor.runOnCompletion(responseFuture, (createPartitionResponse, createPartitionError) ->
                {
                    if (createPartitionError != null)
                    {
                        LOG.error("Error while creating partition {}", partitionId, createPartitionError);
                    }
                    else
                    {
                        LOG.debug("Partition created");
                    }
                });
            }
            else
            {
                LOG.error("Problem in resolving next node address to send partition create request.", throwable);
            }
        });

    }

    @Override
    public Void get()
    {
        return null;
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

    public Injector<ClientTransport> getManagmentClientApiInjector()
    {
        return managmentClientApiInjector;
    }
}
