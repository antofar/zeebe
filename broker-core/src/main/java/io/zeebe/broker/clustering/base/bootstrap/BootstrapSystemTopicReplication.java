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
package io.zeebe.broker.clustering.base.bootstrap;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.orchestration.topic.TopicEvent;
import io.zeebe.broker.clustering.orchestration.topic.TopicState;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;

/**
 * This service only writes a create topic event to the system topic, which then is picked up
 * by the corresponding services to ensure the replication factor.
 */
class BootstrapSystemTopicReplication implements Service<Void>
{

    private final Injector<Partition> partitionInjector = new Injector<>();

    @Override
    public Void get()
    {
        return null;
    }

    @Override
    public void start(final ServiceStartContext startContext)
    {
        final Partition partition = partitionInjector.getValue();
        final PartitionInfo partitionInfo = partition.getInfo();

        final BrokerEventMetadata metadata = new BrokerEventMetadata();
        metadata.eventType(EventType.TOPIC_EVENT);

        final TopicEvent topicEvent = new TopicEvent();
        topicEvent.setState(TopicState.CREATE);
        topicEvent.setName(partitionInfo.getTopicNameBuffer());
        topicEvent.setReplicationFactor(partitionInfo.getReplicationFactor());
        topicEvent.setPartitions(1);

        final LogStreamWriterImpl writer = new LogStreamWriterImpl(partition.getLogStream());
        final long pos = writer.positionAsKey()
              .metadataWriter(metadata)
              .valueWriter(topicEvent)
              .tryWrite();

        if (pos < 0)
        {
            Loggers.CLUSTERING_LOGGER.error("Unable to write {} create topic event on bootstrap", partitionInfo);
        }
    }

    public Injector<Partition> getPartitionInjector()
    {
        return partitionInjector;
    }
}
