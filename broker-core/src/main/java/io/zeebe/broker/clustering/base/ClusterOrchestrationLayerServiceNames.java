package io.zeebe.broker.clustering.base;

import io.zeebe.broker.clustering.orchestration.generation.IdGenerator;
import io.zeebe.broker.clustering.orchestration.state.OrchestrationInstallService;
import io.zeebe.broker.clustering.orchestration.state.OrchestrationService;
import io.zeebe.servicecontainer.ServiceName;

public class ClusterOrchestrationLayerServiceNames
{

    public static final ServiceName<Void> CLUSTERING_ORCHESTRATION_LAYER = ServiceName.newServiceName("cluster.orchestration.bootstrapped", Void.class);

    public static final ServiceName<IdGenerator> ID_GENERATOR_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.id.generator", IdGenerator.class);

    public static final ServiceName<OrchestrationInstallService> ORCHESTRATION_INSTALL_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.install", OrchestrationInstallService.class);
    public static final ServiceName<OrchestrationService> ORCHESTRATION_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration", OrchestrationService.class);
}
