package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class NodeLoad
{
    private final NodeInfo nodeInfo;
    private Set<PartitionInfo> load;

    public NodeLoad(NodeInfo nodeInfo)
    {
        this.nodeInfo = nodeInfo;
        this.load = new HashSet<>();
    }

    public NodeInfo getNodeInfo()
    {
        return nodeInfo;
    }

    public Set<PartitionInfo> getLoad()
    {
        return load;
    }

    public boolean addPartition(PartitionInfo partitionInfo)
    {
        return load.add(partitionInfo);
    }

    @Override
    public String toString()
    {
        return "NodeLoad{" +
            "nodeInfo=" + nodeInfo +
            ", load=" + load +
            '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        NodeLoad nodeLoad = (NodeLoad) o;
        return Objects.equals(nodeInfo, nodeLoad.nodeInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(nodeInfo);
    }
}
