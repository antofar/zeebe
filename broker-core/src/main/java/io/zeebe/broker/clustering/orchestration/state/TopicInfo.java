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
package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.clustering.orchestration.topic.TopicEvent;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class TopicInfo
{

    private final DirectBuffer topicName;
    private final String topicNameString;
    private final long createEventPosition;
    private final int partitionCount;
    private final int replicationFactor;
    private Set<Integer> partitionIds = new HashSet<>();

    public TopicInfo(long position, final TopicEvent event)
    {
        this.topicName = BufferUtil.cloneBuffer(event.getName());
        this.topicNameString = BufferUtil.bufferAsString(event.getName());
        this.createEventPosition = position;
        this.partitionCount = event.getPartitions();
        this.replicationFactor = event.getReplicationFactor();
        event.getPartitionIds().forEach(id -> partitionIds.add(id.getValue()));
    }

    public DirectBuffer getTopicName()
    {
        return topicName;
    }

    public int getPartitionCount()
    {
        return partitionCount;
    }

    public int getReplicationFactor()
    {
        return replicationFactor;
    }

    public Set<Integer> getPartitionIds()
    {
        return partitionIds;
    }

    public TopicInfo addPartitionId(final int partitionId)
    {
        partitionIds.add(partitionId);
        return this;
    }

    public long getCreateEventPosition()
    {
        return createEventPosition;
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        final TopicInfo topicInfo = (TopicInfo) o;
        return partitionCount == topicInfo.partitionCount && replicationFactor == topicInfo.replicationFactor && topicName.equals(topicName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(topicName, partitionCount, replicationFactor);
    }

    @Override
    public String toString()
    {
        return "TopicInfo{" + "topicName=" + topicNameString + ", partitionCount=" + partitionCount + ", replicationFactor=" + replicationFactor + ", partitionIds=" + partitionIds + '}';
    }

    public void update(final TopicInfo topicInfo)
    {
        partitionIds = topicInfo.partitionIds;
    }
}
