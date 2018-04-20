package io.zeebe.broker.clustering.orchestration.state;

public class CreatePartitionCommand
{

    private final String topicName;
    private final int replicationCount;

    public CreatePartitionCommand(String topicName, int replicationCount)
    {
        this.topicName = topicName;
        this.replicationCount = replicationCount;
    }
}
