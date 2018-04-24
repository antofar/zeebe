package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class TopicInfo
{

    private final DirectBuffer topicName;
    private final int partitionCount;
    private final int replicationFactor;
    private Set<Integer> partitionIds = new HashSet<>();

    public TopicInfo(final TopicEvent event)
    {
        this.topicName = BufferUtil.cloneBuffer(event.getName());
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
        return "TopicInfo{" + "topicName=" + topicName + ", partitionCount=" + partitionCount + ", replicationFactor=" + replicationFactor + ", partitionIds="
            + partitionIds + '}';
    }

    public void update(final TopicInfo topicInfo)
    {
        partitionIds = topicInfo.partitionIds;
    }
}
