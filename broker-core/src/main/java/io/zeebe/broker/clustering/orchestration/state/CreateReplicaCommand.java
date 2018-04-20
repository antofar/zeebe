package io.zeebe.broker.clustering.orchestration.state;

public class CreateReplicaCommand
{

    private final String topicName;
    private final int replicationCount;

    public CreateReplicaCommand(int replicationCount)
    {
        this.replicationCount = replicationCount;
    }
}
