package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.servicecontainer.ServiceName;

public class ClusterOrchestrationLayerSerivceNames
{

    public static final ServiceName<Void> CLUSTER_TOPIC_STATE_INSTALL_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.clusterTopicState.install", Void.class);
    public static final ServiceName<ClusterTopicState> CLUSTER_TOPIC_STATE_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.clusterTopicState", ClusterTopicState.class);

}
