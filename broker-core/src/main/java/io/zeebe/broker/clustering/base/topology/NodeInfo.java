package io.zeebe.broker.clustering.base.topology;

import io.zeebe.transport.SocketAddress;

import java.util.ArrayList;
import java.util.List;

public class NodeInfo
{
    private final SocketAddress apiPort;
    private final SocketAddress managementPort;
    private final SocketAddress replicationPort;

    private final List<PartitionInfo> leader = new ArrayList<>();
    private final List<PartitionInfo> follower = new ArrayList<>();

    public NodeInfo(SocketAddress apiPort,
                    SocketAddress managementPort,
                    SocketAddress replicationPort)
    {
        this.apiPort = apiPort;
        this.managementPort = managementPort;
        this.replicationPort = replicationPort;
    }

    public SocketAddress getApiPort()
    {
        return apiPort;
    }

    public SocketAddress getManagementPort()
    {
        return managementPort;
    }

    public SocketAddress getReplicationPort()
    {
        return replicationPort;
    }

    public List<PartitionInfo> getLeader()
    {
        return leader;
    }

    public List<PartitionInfo> getFollower()
    {
        return follower;
    }

    @Override
    public String toString()
    {
        return String.format("Node{clientApi=%s, managementApi=%s, replicationApi=%s}", apiPort, managementPort, replicationPort);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((apiPort == null) ? 0 : apiPort.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final NodeInfo other = (NodeInfo) obj;
        if (apiPort == null)
        {
            if (other.apiPort != null)
            {
                return false;
            }
        }
        else if (!apiPort.equals(other.apiPort))
        {
            return false;
        }
        return true;
    }
}
