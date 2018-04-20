package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ScheduledTimer;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;

import java.time.Duration;
import java.util.*;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.SYSTEM_CREATE_TOPIC_PROCESSOR_ID;

public class OrchestrationService implements Service<OrchestrationService>, TypedEventProcessor<TopicEvent>
{

    private final Injector<TopologyManager> topologyManagerServiceInjector = new Injector<>();
    private final Injector<Partition> leaderSystemPartitionInjector = new Injector<>();
    private final Injector<ClientTransport> managmentClientTransportInjector = new Injector<>();
    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();

    private final Map<String, TopicInfo> state = new HashMap<>();
    private ScheduledTimer scheduledTimer;
    private Partition partition;
    private TopologyManager topologyManager;
    private ClientTransport managmentClientTransport;
    private ActorControl actor;


    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        actor = streamProcessor.getActor();
        actor.runAtFixedRate(Duration.ofSeconds(1), this::checkCurrentState);
    }

    @Override
    public void processEvent(TypedEvent<TopicEvent> event)
    {
        final TopicEvent topicEvent = event.getValue();

        final String topicName = BufferUtil.bufferAsString(topicEvent.getName());

        if (state.containsKey(topicName) ||
            topicEvent.getPartitions() < 1 ||
            topicEvent.getReplicationFactor() < 1)
        {
            topicEvent.setState(TopicState.CREATE_REJECTED);
        }

    }

    @Override
    public boolean executeSideEffects(TypedEvent<TopicEvent> event, TypedResponseWriter responseWriter)
    {
        if (event.getValue().getState() == TopicState.CREATE_REJECTED)
        {
            return responseWriter.write(event);
        }
        else
        {
            return true;
        }
    }

    @Override
    public long writeEvent(TypedEvent<TopicEvent> event, TypedStreamWriter writer)
    {
        final TopicEvent value = event.getValue();
        long position = 0;
        if (value.getState() == TopicState.CREATE_REJECTED)
        {
            position = writer.writeFollowupEvent(event.getKey(), event.getValue());
        }
        return position;
    }

    @Override
    public void updateState(TypedEvent<TopicEvent> event)
    {
        final TopicEvent value = event.getValue();
        if (value.getState() != TopicState.CREATE_REJECTED)
        {
            final String topicName = BufferUtil.bufferAsString(value.getName());
            state.put(topicName, new TopicInfo(topicName, value.getPartitions(), value.getReplicationFactor()));
        }
    }

    @Override
    public void start(ServiceStartContext startContext)
    {

        topologyManager = topologyManagerServiceInjector.getValue();
        partition = leaderSystemPartitionInjector.getValue();
        managmentClientTransport = managmentClientTransportInjector.getValue();
        final StreamProcessorServiceFactory streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();

        final TypedStreamEnvironment typedStreamEnvironment = new TypedStreamEnvironment(partition.getLogStream(), clientApiTransportInjector.getValue().getOutput());

        final TypedStreamProcessor typedStreamProcessor = typedStreamEnvironment.newStreamProcessor()
            .onEvent(EventType.TOPIC_EVENT, TopicState.CREATE, this)
            .build();


        streamProcessorServiceFactory.createService(partition, leaderSystemPartitionInjector.getInjectedServiceName())
            .additionalDependencies(startContext.getServiceName())
            .processor(typedStreamProcessor)
            .processorId(SYSTEM_CREATE_TOPIC_PROCESSOR_ID)
            .processorName("orchestrator")
            .build();
    }

    @Override
    public OrchestrationService get()
    {
        return this;
    }

    private void checkCurrentState()
    {

        final ActorFuture<Map<String, Map<Integer, Integer>>> resultFuture = topologyManager.query(readableTopology -> {

            final Map<String, Map<Integer, Integer>> currentState = new HashMap<>();


            final Collection<PartitionInfo> partitions = readableTopology.getPartitions();
            for (PartitionInfo partitionInfo : partitions)
            {
                final int partitionId = partitionInfo.getPartitionId();
                final DirectBuffer topicName = partitionInfo.getTopicName();

                int replication = 0;
                final List<NodeInfo> followers = readableTopology.getFollowers(partitionId);
                replication += followers == null ? 0 : followers.size();

                final NodeInfo leader = readableTopology.getLeader(partitionId);
                replication += leader == null ? 0 : 1;

                final Map<Integer, Integer> partitionReplication = currentState.computeIfAbsent(BufferUtil.bufferAsString(topicName), (s) -> new HashMap<>());
                partitionReplication.put(partitionId, replication);
            }

            return currentState;
        });

        actor.runOnCompletion(resultFuture, (currentState, throwable) ->
        {
            if (throwable != null)
            {
                for (TopicInfo topicInfo : state.values())
                {
                    final String topicName = topicInfo.getName();
                    final Map<Integer, Integer> partitionReplication = currentState.get(topicName);
                    if (partitionReplication != null)
                    {
                        for (int i = partitionReplication.size(); i < topicInfo.getPartitionCount(); i++)
                        {
                            // create missing partition
                            new CreatePartitionCommand(topicName, topicInfo.getReplicationFactor());
                        }

                        for (Map.Entry<Integer, Integer> replication : partitionReplication.entrySet())
                        {
                            // create missing replication
                            final int diff = topicInfo.getReplicationFactor() - replication.getValue();
                            new CreateReplicaCommand(diff);

//                            for (int i = replication.getValue(); i < topicInfo.getReplicationFactor(); i++)
//                            {
//                                // create missing replication
//                            }
                        }
                    }
                }

            }
        });


//            final List<Request> requests = new ArrayList<>();

//            final Collection<PartitionInfo> partitions = readableTopology.getPartitions();
//            for (PartitionInfo partitionInfo : partitions)
//            {
//                final int partitionId = partitionInfo.getPartitionId();
//                final DirectBuffer topicName = partitionInfo.getTopicName();
//
//                final TopicInfo topicInfo = stateCopy.get(topicName);
//
//                int replication = 0;
//                final List<NodeInfo> followers = readableTopology.getFollowers(partitionId);
//                replication += followers == null ? 0 : followers.size();
//
//                final NodeInfo leader = readableTopology.getLeader(partitionId);
//                replication += leader == null ? 0 : 1;
//
//                // needed replication to reach desired state
//                if (replication < topicInfo.getReplicationFactor())
//                {
//                    // need replication for this partition !
//                    // create replication request
//                    requests.add(new Request(partitionId)
//                    {
//                        @Override
//                        public void request()
//                        {
//                            // replication request
//                        }
//                    });
//                }
//
//                topicInfo.decrementPartitionCount();
//
//                if (topicInfo.getPartitionCount() == 0)
//                {
//                    stateCopy.remove(BufferUtil.bufferAsString(topicName));
//                }
//            }
//
//            // for each topic which is still in the map create partition request
//            for (String topicName : stateCopy.keySet())
//            {
//                final TopicInfo info = stateCopy.get(topicName);
//                while (info.getPartitionCount() != 0)
//                {
//                    // id
//                    requests.add(new Request())
//                    info.decrementPartitionCount();
//                }
//            }

//            return requests;
//        });



    }


    public void updateState(TopicInfo event)
    {
        actor.run(() ->
        {
            state.put(event.getName(), event);
        });
    }

    public Injector<TopologyManager> getTopologyManagerServiceInjector()
    {
        return topologyManagerServiceInjector;
    }

    public Injector<Partition> getLeaderSystemPartitionInjector()
    {
        return leaderSystemPartitionInjector;
    }

    public Injector<ClientTransport> getManagmentClientTransportInjector()
    {
        return managmentClientTransportInjector;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }
}
