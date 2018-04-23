package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.transport.SocketAddress;

public class ClusterPartitionState
{
    private int replicationCount;
    private SocketAddress leader;

    public int getReplicationCount()
    {
        return replicationCount;
    }

    public void setReplicationCount(final int replicationCount)
    {
        this.replicationCount = replicationCount;
    }

    public SocketAddress getLeader()
    {
        return leader;
    }

    public void setLeader(final SocketAddress leader)
    {
        this.leader = leader;
    }

    @Override
    public String toString()
    {
        return "ClusterPartitionState{" +
            "replicationCount=" + replicationCount +
            ", leader=" + leader +
            '}';
    }
}
