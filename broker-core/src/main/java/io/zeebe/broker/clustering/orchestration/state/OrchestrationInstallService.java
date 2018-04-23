package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerServiceNames;
import io.zeebe.broker.transport.TransportServiceNames;
import io.zeebe.servicecontainer.*;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.TOPOLOGY_MANAGER_SERVICE;
import static io.zeebe.broker.clustering.orchestration.ClusterOrchestrationLayerServiceNames.ID_GENERATOR_SERVICE_NAME;
import static io.zeebe.broker.logstreams.LogStreamServiceNames.STREAM_PROCESSOR_SERVICE_FACTORY;
import static io.zeebe.broker.transport.TransportServiceNames.CLIENT_API_SERVER_NAME;
import static io.zeebe.broker.transport.TransportServiceNames.serverTransport;

public class OrchestrationInstallService implements Service<Void>
{
    private final ServiceGroupReference<Partition> systemLeaderGroupReference = ServiceGroupReference.<Partition>create()
        .onAdd(this::installOrchestrationService)
        .build();

    private ServiceStartContext startContext;

    @Override
    public void start(ServiceStartContext startContext)
    {
        this.startContext = startContext;
        Loggers.CLUSTERING_LOGGER.debug("Starting orchestration install service");
    }

    private void installOrchestrationService(ServiceName<Partition> leaderSystemPartitionName, Partition leaderSystemPartition)
    {
        final OrchestrationService orchestrationService = new OrchestrationService();
        startContext.createService(ClusterOrchestrationLayerServiceNames.ORCHESTRATION_SERVICE_NAME, orchestrationService)
                    .dependency(ID_GENERATOR_SERVICE_NAME, orchestrationService.getIdGeneratorInjector())
                    .dependency(leaderSystemPartitionName, orchestrationService.getLeaderSystemPartitionInjector())
                    .dependency(TOPOLOGY_MANAGER_SERVICE, orchestrationService.getTopologyManagerServiceInjector())
                    .dependency(TransportServiceNames.clientTransport(TransportServiceNames.MANAGEMENT_API_CLIENT_NAME), orchestrationService.getManagmentClientTransportInjector())
                    .dependency(serverTransport(CLIENT_API_SERVER_NAME), orchestrationService.getClientApiTransportInjector())
                    .dependency(STREAM_PROCESSOR_SERVICE_FACTORY, orchestrationService.getStreamProcessorServiceFactoryInjector())
                    .install();

        Loggers.CLUSTERING_LOGGER.debug("Installing orchestration service");
    }

    public ServiceGroupReference<Partition> getSystemLeaderGroupReference()
    {
        return systemLeaderGroupReference;
    }

    @Override
    public Void get()
    {
        return null;
    }
}
