package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.clustering.base.topology.NodeInfo;

public class ClusterPartitionState
{
    private int replicationCount;
    private NodeInfo leader;

    public int getReplicationCount()
    {
        return replicationCount;
    }

    public void setReplicationCount(final int replicationCount)
    {
        this.replicationCount = replicationCount;
    }

    public NodeInfo getLeader()
    {
        return leader;
    }

    public void setLeader(final NodeInfo leader)
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
