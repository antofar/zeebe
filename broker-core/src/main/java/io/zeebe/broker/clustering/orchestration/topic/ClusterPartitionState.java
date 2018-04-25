package io.zeebe.broker.clustering.orchestration.topic;

import java.util.*;

import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.ReadableTopology;
import org.agrona.DirectBuffer;

public class ClusterPartitionState
{

    final Map<DirectBuffer, List<PartitionInfo>> state = new HashMap<>();

    public static ClusterPartitionState computeCurrentState(final ReadableTopology topology)
    {
        final ClusterPartitionState currentState = new ClusterPartitionState();
        topology.getPartitions().forEach(currentState::addPartition);
        return currentState;
    }

    public void addPartition(final PartitionInfo partitionInfo)
    {
        state.computeIfAbsent(partitionInfo.getTopicName(), t -> new ArrayList<>())
             .add(partitionInfo);
    }

    public List<PartitionInfo> getPartitions(final DirectBuffer topicName)
    {
        return state.getOrDefault(topicName, Collections.emptyList());
    }
}
