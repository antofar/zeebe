package io.zeebe.broker.clustering.orchestration.topic;

import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;

import java.util.ArrayList;
import java.util.List;

public class PartitionNodes
{

    private final PartitionInfo partitionInfo;
    private NodeInfo leader;
    private final List<NodeInfo> followers;

    private final List<NodeInfo> nodes;

    public PartitionNodes(PartitionInfo partitionInfo)
    {
        this.partitionInfo = partitionInfo;
        this.followers = new ArrayList<>();
        this.nodes = new ArrayList<>();
    }

    public int getPartitionId()
    {
        return partitionInfo.getPartitionId();
    }

    public PartitionInfo getPartitionInfo()
    {
        return partitionInfo;
    }

    public List<NodeInfo> getFollowers()
    {
        return followers;
    }

    public void addFollowers(List<NodeInfo> newNodes)
    {
        followers.addAll(newNodes);
        nodes.addAll(newNodes);
    }

    public NodeInfo getLeader()
    {
        return leader;
    }

    public void setLeader(NodeInfo newLeader)
    {
        if (leader != null)
        {
            nodes.remove(leader);
        }
        this.leader = newLeader;
        nodes.add(newLeader);
    }

    public List<NodeInfo> getNodes()
    {
        return nodes;
    }
}
