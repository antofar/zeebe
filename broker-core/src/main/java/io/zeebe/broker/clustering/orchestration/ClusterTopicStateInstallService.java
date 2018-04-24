package io.zeebe.broker.clustering.orchestration;

import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerSerivceNames.CLUSTER_TOPIC_STATE_SERVICE_NAME;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.STREAM_PROCESSOR_SERVICE_FACTORY;
import static io.zeebe.broker.transport.TransportServiceNames.CLIENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.serverTransport;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import org.slf4j.Logger;

public class ClusterTopicStateInstallService implements Service<Void>
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
        final ClusterTopicState clusterTopicState = new ClusterTopicState();
        startContext.createService(CLUSTER_TOPIC_STATE_SERVICE_NAME, clusterTopicState)
                    .dependency(partitionServiceName, clusterTopicState.getPartitionInjector())
                    .dependency(serverTransport(CLIENT_API_SERVER_NAME), clusterTopicState.getServerTransportInjector())
                    .dependency(STREAM_PROCESSOR_SERVICE_FACTORY, clusterTopicState.getStreamProcessorServiceFactoryInjector())
                    .install();

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
