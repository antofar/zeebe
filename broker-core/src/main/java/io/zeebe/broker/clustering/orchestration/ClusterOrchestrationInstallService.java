package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.orchestration.id.IdGenerator;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.broker.transport.controlmessage.ControlMessageHandlerManager;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.ServerTransport;
import org.slf4j.Logger;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerSerivceNames.*;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.STREAM_PROCESSOR_SERVICE_FACTORY;
import static io.zeebe.broker.transport.TransportServiceNames.CLIENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.serverTransport;

public class ClusterOrchestrationInstallService implements Service<Void>
{

    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final Injector<ControlMessageHandlerManager> controlMessageHandlerManagerInjector = new Injector<>();
    private final Injector<ServerTransport> transportInjector = new Injector<>();

    private final ServiceGroupReference<Partition> systemLeaderGroupReference = ServiceGroupReference.<Partition>create()
        .onAdd(this::installCreateTopicStateService)
        .build();

    private ServiceStartContext startContext;
    private RequestPartitionsMessageHandler requestPartitionsMessageHandler;

    @Override
    public void start(final ServiceStartContext startContext)
    {
        this.startContext = startContext;

        final ServerOutput serverOutput = transportInjector.getValue().getOutput();
        requestPartitionsMessageHandler = new RequestPartitionsMessageHandler(serverOutput);

        final ControlMessageHandlerManager controlMessageHandlerManager = controlMessageHandlerManagerInjector.getValue();
        controlMessageHandlerManager.registerHandler(requestPartitionsMessageHandler);
    }

    private void installCreateTopicStateService(final ServiceName<Partition> partitionServiceName, final Partition partition)
    {
        final CompositeServiceBuilder compositeInstall = startContext.createComposite(CLUSTER_ORCHESTRATION_COMPISITE_SERVICE_NAME);

        final ClusterTopicState clusterTopicState = new ClusterTopicState();
        compositeInstall.createService(CLUSTER_TOPIC_STATE_SERVICE_NAME, clusterTopicState)
                    .dependency(partitionServiceName, clusterTopicState.getPartitionInjector())
                    .dependency(serverTransport(CLIENT_API_SERVER_NAME), clusterTopicState.getServerTransportInjector())
                    .dependency(STREAM_PROCESSOR_SERVICE_FACTORY, clusterTopicState.getStreamProcessorServiceFactoryInjector())
                    .install();

        final IdGenerator idGenerator = new IdGenerator();
        compositeInstall.createService(ID_GENERATOR_SERVICE_NAME, idGenerator)
                        .dependency(serverTransport(CLIENT_API_SERVER_NAME), idGenerator.getClientApiTransportInjector())
                        .dependency(STREAM_PROCESSOR_SERVICE_FACTORY, idGenerator.getStreamProcessorServiceFactoryInjector())
                        .dependency(partitionServiceName, idGenerator.getPartitionInjector())
                        .install();

        final TopicCreationReviserService topicCreationReviserService = new TopicCreationReviserService();
        compositeInstall.createService(TOPIC_CREATION_REVISER_SERVICE_NAME, topicCreationReviserService)
                        .dependency(CLUSTER_TOPIC_STATE_SERVICE_NAME, topicCreationReviserService.getStateInjector())
                        .dependency(TOPOLOGY_MANAGER_SERVICE, topicCreationReviserService.getTopologyManagerInjector())
                        .dependency(partitionServiceName, topicCreationReviserService.getLeaderSystemPartitionInjector())
                        .dependency(ID_GENERATOR_SERVICE_NAME, topicCreationReviserService.getIdGeneratorInjector())
                        .install();

        compositeInstall.createService(REQUEST_PARTITIONS_MESSAGE_HANDLER_SERVICE_NAME, requestPartitionsMessageHandler)
                        .dependency(CLUSTER_TOPIC_STATE_SERVICE_NAME, requestPartitionsMessageHandler.getClusterTopicStateInjector())
                        .install();

        final NodeOrchestratingService nodeOrchestratingService = new NodeOrchestratingService();
        compositeInstall.createService(NODE_ORCHESTRATING_SERVICE_NAME, nodeOrchestratingService)
                        .dependency(TOPOLOGY_MANAGER_SERVICE, nodeOrchestratingService.getTopologyManagerInjector())
                        .install();

        compositeInstall.install();

        LOG.debug("Installing cluster topic state service");
    }

    @Override
    public Void get()
    {
        return null;
    }

    public Injector<ControlMessageHandlerManager> getControlMessageHandlerManagerInjector()
    {
        return controlMessageHandlerManagerInjector;
    }

    public Injector<ServerTransport> getTransportInjector()
    {
        return transportInjector;
    }

    public ServiceGroupReference<Partition> getSystemLeaderGroupReference()
    {
        return systemLeaderGroupReference;
    }

}
