package io.zeebe.broker.clustering.base.topology;

import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

public class PartitionInfo
{
    private final DirectBuffer topicName;
    private final int paritionId;
    private final int replicationFactor;

    public PartitionInfo(DirectBuffer topicName, int paritionId, int replicationFactor)
    {
        this.topicName = topicName;
        this.paritionId = paritionId;
        this.replicationFactor = replicationFactor;
    }

    public DirectBuffer getTopicName()
    {
        return topicName;
    }

    public String getTopicNameAsString()
    {
        return BufferUtil.bufferAsString(topicName);
    }

    public int getPartitionId()
    {
        return paritionId;
    }

    public int getReplicationFactor()
    {
        return replicationFactor;
    }

    @Override
    public String toString()
    {
        return String.format("Partition{topic=%s, partitionId=%d, replicationFactor=%d}", getTopicNameAsString(), paritionId, replicationFactor);
    }
}
