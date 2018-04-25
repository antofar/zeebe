package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.servicecontainer.ServiceName;

public class ClusterOrchestrationLayerSerivceNames
{

    public static final ServiceName<Void> CLUSTER_ORCHESTRATION_INSTALL_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.install", Void.class);
    public static final ServiceName<Void> CLUSTER_ORCHESTRATION_COMPISITE_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.composite", Void.class);

    public static final ServiceName<ClusterTopicState> CLUSTER_TOPIC_STATE_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.clusterTopicState", ClusterTopicState.class);

    public static final ServiceName<Void> TOPIC_CREATION_REVISER_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.topic.reviser", Void.class);



}
