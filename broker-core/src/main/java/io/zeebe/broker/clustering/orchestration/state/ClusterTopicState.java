package io.zeebe.broker.clustering.orchestration.state;

import java.util.*;
import java.util.stream.Collectors;

import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.transport.SocketAddress;

public class ClusterTopicState
{
    private final Map<String, Map<Integer, ClusterPartitionState>> topicPartitonReplications = new HashMap<>();
    private final Map<SocketAddress, Integer> brokerUsage = new HashMap<>();


    public void setReplicationCount(final String topicName, final int partitionId, final int replicationCount)
    {
        topicPartitonReplications.computeIfAbsent(topicName, (n) -> new HashMap<>())
                                 .computeIfAbsent(partitionId, (p) -> new ClusterPartitionState())
                                 .setReplicationCount(replicationCount);
    }

    public void setLeader(final String topicName, final int partitionId, final NodeInfo leader)
    {
        topicPartitonReplications.computeIfAbsent(topicName, (n) -> new HashMap<>())
                                 .computeIfAbsent(partitionId, (p) -> new ClusterPartitionState())
                                 .setLeader(leader);
    }

    public void increaseBrokerUsage(final SocketAddress socketAddress)
    {
        brokerUsage.compute(socketAddress, (a, v) -> v != null ? v + 1 : 1);
    }

    public void addBroker(final SocketAddress socketAddress)
    {
        brokerUsage.putIfAbsent(socketAddress, 0);
    }

    public List<SocketAddress> nextSocketAddress(final SocketAddress skipAddress, int count)
    {

        final List<SocketAddress> socketAddresses = new ArrayList<>(count);
        int neededAddresses = count;
        while (socketAddresses.size() < count)
        {
            final List<SocketAddress> addresses = brokerUsage.entrySet()
                .stream()
                .filter(e -> !e.getKey().equals(skipAddress))
                .sorted(Comparator.comparing(Map.Entry::getValue))
                .map(Map.Entry::getKey)
                .distinct()
                .limit(neededAddresses)
                .collect(Collectors.toList());
            neededAddresses -= addresses.size();
            socketAddresses.addAll(addresses);
        }

        return socketAddresses;
    }

    public Map<Integer, ClusterPartitionState> getPartitionReplications(String topicName)
    {
        return topicPartitonReplications.getOrDefault(topicName, new HashMap<>());
    }

    @Override
    public String toString()
    {
        return "ClusterTopicState{" +
            "topicPartitonReplications=" + topicPartitonReplications +
            ", brokerUsage=" + brokerUsage +
            '}';
    }
}
