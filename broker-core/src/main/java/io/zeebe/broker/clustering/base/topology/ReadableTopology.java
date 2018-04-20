package io.zeebe.broker.clustering.base.topology;

import io.zeebe.transport.SocketAddress;

import java.util.Collection;
import java.util.List;

public interface ReadableTopology
{

    NodeInfo getLocal();

    NodeInfo getMemberByAddress(SocketAddress apiAddress);

    List<NodeInfo> getMembers();

    PartitionInfo getParition(int partitionId);

    NodeInfo getLeader(int partitionId);

    List<NodeInfo> getFollowers(int partitionId);

    Collection<PartitionInfo> getPartitions();
}
