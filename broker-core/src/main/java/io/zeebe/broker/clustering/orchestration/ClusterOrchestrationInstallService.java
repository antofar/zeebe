package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.servicecontainer.*;
import org.slf4j.Logger;

import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerSerivceNames.*;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.STREAM_PROCESSOR_SERVICE_FACTORY;
import static io.zeebe.broker.transport.TransportServiceNames.CLIENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.serverTransport;

public class ClusterOrchestrationInstallService implements Service<Void>
{

    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final ServiceGroupReference<Partition> systemLeaderGroupReference = ServiceGroupReference.<Partition>create()
        .onAdd(this::installCreateTopicStateService)
        .build();

    private ServiceStartContext startContext;

    @Override
    public void start(final ServiceStartContext startContext)
    {
        this.startContext = startContext;
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

        final TopicCreationReviserService topicCreationReviserService = new TopicCreationReviserService();
        compositeInstall.createService(TOPIC_CREATION_REVISER_SERVICE_NAME, topicCreationReviserService)
                        .dependency(CLUSTER_TOPIC_STATE_SERVICE_NAME, topicCreationReviserService.getStateInjector())
                        .dependency(ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE, topicCreationReviserService.getTopologyManagerInjector())
                        .install();

        compositeInstall.install();

        LOG.debug("Installing cluster topic state service");
    }

    @Override
    public Void get()
    {
        return null;
    }

    public ServiceGroupReference<Partition> getSystemLeaderGroupReference()
    {
        return systemLeaderGroupReference;
    }
}
