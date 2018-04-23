package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.clustering.orchestration.generation.IdGenerator;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ScheduledTimer;
import io.zeebe.util.sched.future.ActorFuture;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.SYSTEM_CREATE_TOPIC_PROCESSOR_ID;

public class OrchestrationService implements Service<OrchestrationService>, TypedEventProcessor<TopicEvent>
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    private static final Duration PENDING_COMMAND_TIMEOUT = Duration.ofSeconds(15);

    private final Injector<IdGenerator> idGeneratorInjector = new Injector<>();
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


    private final Set<OrchestrationCommand> pendingCommands = new HashSet<>();
    private IdGenerator idGenerator;

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        actor = streamProcessor.getActor();
        LOG.debug("Orchestration service log stream processor open");
    }

    @Override
    public void onRecovered()
    {
        final Duration delay = Duration.ofSeconds(1);
        scheduledTimer = actor.runAtFixedRate(delay, this::checkCurrentState);
        LOG.debug("Orchestration service schedules current state check every {}", delay);
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
            LOG.warn("Rejecting create topic command: {}", topicEvent);
            topicEvent.setState(TopicState.CREATE_REJECTED);
        }
        else
        {
            topicEvent.setState(TopicState.CREATED);
        }
    }

    @Override
    public boolean executeSideEffects(TypedEvent<TopicEvent> event, TypedResponseWriter responseWriter)
    {
        return responseWriter.write(event);
    }

    @Override
    public long writeEvent(TypedEvent<TopicEvent> event, TypedStreamWriter writer)
    {
        long position = 0;
        position = writer.writeFollowupEvent(event.getKey(), event.getValue());
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
            LOG.debug("Added topic to desired state: {}", value);
        }
    }

    @Override
    public void start(ServiceStartContext startContext)
    {

        topologyManager = topologyManagerServiceInjector.getValue();
        partition = leaderSystemPartitionInjector.getValue();
        managmentClientTransport = managmentClientTransportInjector.getValue();
        idGenerator = idGeneratorInjector.getValue();
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

        LOG.debug("Orchestration service starting");
    }

    @Override
    public OrchestrationService get()
    {
        return this;
    }

    private void checkCurrentState()
    {
        final ActorFuture<ClusterTopicState> resultFuture = topologyManager.query(readableTopology ->
        {
            final ClusterTopicState currentState = new ClusterTopicState();

            for (NodeInfo member : readableTopology.getMembers())
            {
                currentState.addBroker(member.getManagementPort());
            }

            final Collection<PartitionInfo> partitions = readableTopology.getPartitions();
            for (final PartitionInfo partitionInfo : partitions)
            {
                final int partitionId = partitionInfo.getPartitionId();
                final String topicName = BufferUtil.bufferAsString(partitionInfo.getTopicName());

                int replicationCount = 0;
                final List<NodeInfo> followers = readableTopology.getFollowers(partitionId);
                if (followers != null)
                {
                    replicationCount += followers.size();
                    for (final NodeInfo follower : followers)
                    {
                        currentState.increaseBrokerUsage(follower.getManagementPort());
                    }
                }

                final NodeInfo leader = readableTopology.getLeader(partitionId);
                if (leader != null)
                {
                    replicationCount += 1;
                    currentState.increaseBrokerUsage(leader.getManagementPort());
                    currentState.setLeader(topicName, partitionId, leader.getReplicationPort());
                }

                currentState.setReplicationCount(topicName, partitionId, replicationCount);
            }

            return currentState;
        });

        actor.runOnCompletion(resultFuture, (currentState, throwable) ->
        {
            LOG.info("Current state: {}", currentState);
            LOG.info("Desired state: {}", state);
            if (throwable == null)
            {
                for (final OrchestrationCommand pendingCommand : pendingCommands)
                {
                    for (final RemoteAddress remoteAddress  : pendingCommand.getRemoteAddresses())
                    {
                        currentState.increaseBrokerUsage(remoteAddress.getAddress());
                    }
                }

                final List<OrchestrationCommand> commands = new ArrayList<>();

                for (final TopicInfo topicInfo : state.values())
                {
                    final String topicName = topicInfo.getName();
                    final Map<Integer, ClusterPartitionState> partitionReplication = currentState.getPartitionReplications(topicName);
                    final int missingPartititons = topicInfo.getPartitionCount() - partitionReplication.size();

                    if (missingPartititons > 0)
                    {
                        commands.add(new CreatePartitionCommand(topicName, topicInfo.getReplicationFactor(), missingPartititons, actor, idGenerator));
                    }

                    for (final Map.Entry<Integer, ClusterPartitionState> partitionState : partitionReplication.entrySet())
                    {
                        final ClusterPartitionState state = partitionState.getValue();
                        final int missingMembers = topicInfo.getReplicationFactor() - state.getReplicationCount();

                        final SocketAddress leader = state.getLeader();
                        if (missingMembers > 0 && leader != null)
                        {
                            commands.add(new InviteMemberCommand(topicName, partitionState.getKey(), topicInfo.getReplicationFactor(), leader, missingMembers));
                        }
                    }
                }


                executeCommands(currentState::nextSocketAddress, commands);
            }
        });
    }

    private void executeCommands(final Supplier<SocketAddress> socketAddressSupplier, final List<OrchestrationCommand> commands)
    {

        for (final OrchestrationCommand command : commands)
        {
            if (!pendingCommands.contains(command))
            {
                command.execute(managmentClientTransport, socketAddressSupplier);
                pendingCommands.add(command);

                actor.runDelayed(PENDING_COMMAND_TIMEOUT, () ->
                {
                    pendingCommands.remove(command);
                });
            }
        }

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

    public Injector<IdGenerator> getIdGeneratorInjector()
    {
        return idGeneratorInjector;
    }
}
