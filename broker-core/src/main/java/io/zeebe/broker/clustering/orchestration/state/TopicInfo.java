package io.zeebe.broker.clustering.orchestration.state;

public class TopicInfo
{
    private final String name;
    private int partitionCount;
    private final int replicationFactor;

    public TopicInfo(String name, int partitionCount, int replicationFactor)
    {
        this.name = name;
        this.partitionCount = partitionCount;
        this.replicationFactor = replicationFactor;
    }

    public TopicInfo(TopicInfo otherInfo)
    {
        this.name = otherInfo.name;
        this.partitionCount = otherInfo.partitionCount;
        this.replicationFactor = otherInfo.replicationFactor;
    }

    public int getPartitionCount()
    {
        return partitionCount;
    }

    public void decrementPartitionCount()
    {
        partitionCount--;
    }

    public int getReplicationFactor()
    {
        return replicationFactor;
    }

    public String getName()
    {
        return name;
    }


    @Override
    public String toString()
    {
        return "TopicInfo{" + "name='" + name + '\'' + ", partitionCount=" + partitionCount + ", replicationFactor=" + replicationFactor + '}';
    }
}
