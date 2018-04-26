package io.zeebe.broker.clustering.orchestration.topic;

import java.util.*;

import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
import org.agrona.DirectBuffer;

public class ClusterPartitionState
{
    final Map<DirectBuffer, List<PartitionNodes>> state = new HashMap<>();

    public static ClusterPartitionState computeCurrentState(final ReadableTopology topology)
    {
        final ClusterPartitionState currentState = new ClusterPartitionState();
        topology.getPartitions().forEach(partition -> currentState.addPartition(partition, topology));
        return currentState;
    }

    public void addPartition(final PartitionInfo partitionInfo, final ReadableTopology topology)
    {
        final List<PartitionNodes> listOfPartitionNodes = state.computeIfAbsent(partitionInfo.getTopicName(), t -> new ArrayList<>());

        final PartitionNodes newPartitionNodes = new PartitionNodes(partitionInfo);
        newPartitionNodes.setLeader(topology.getLeader(partitionInfo.getPartitionId()));
        newPartitionNodes.addFollowers(topology.getFollowers(partitionInfo.getPartitionId()));

        listOfPartitionNodes.add(newPartitionNodes);
    }

    public List<PartitionNodes> getPartitions(final DirectBuffer topicName)
    {
        return state.getOrDefault(topicName, Collections.emptyList());
    }
}
