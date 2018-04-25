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
package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.clustering.orchestration.id.IdGenerator;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.broker.clustering.orchestration.topic.RequestPartitionsMessageHandler;
import io.zeebe.servicecontainer.ServiceName;

public class ClusterOrchestrationLayerSerivceNames
{

    public static final ServiceName<Void> CLUSTER_ORCHESTRATION_INSTALL_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.install", Void.class);
    public static final ServiceName<Void> CLUSTER_ORCHESTRATION_COMPISITE_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.composite", Void.class);

    public static final ServiceName<ClusterTopicState> CLUSTER_TOPIC_STATE_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.clusterTopicState", ClusterTopicState.class);

    public static final ServiceName<Void> TOPIC_CREATION_REVISER_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.topic.reviser", Void.class);
    public static final ServiceName<RequestPartitionsMessageHandler> REQUEST_PARTITIONS_MESSAGE_HANDLER_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.requestsPartitionsMessageHandler", RequestPartitionsMessageHandler.class);
    public static final ServiceName<IdGenerator> ID_GENERATOR_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.idGenerator", IdGenerator.class);
    public static final ServiceName<NodeOrchestratingService> NODE_ORCHESTRATING_SERVICE_NAME = ServiceName.newServiceName("cluster.orchestration.node", NodeOrchestratingService.class);

}
