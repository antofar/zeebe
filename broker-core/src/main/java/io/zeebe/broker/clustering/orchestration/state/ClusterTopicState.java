package io.zeebe.broker.clustering.orchestration.state;

import java.util.*;

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

    public void setLeader(final String topicName, final int partitionId, final SocketAddress leader)
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

    public SocketAddress nextSocketAddress()
    {
        final Optional<SocketAddress> nextAddress = brokerUsage.entrySet()
                    .stream()
                    .sorted(Comparator.comparing(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .findFirst();

        if (nextAddress.isPresent())
        {
            final SocketAddress address = nextAddress.get();
            increaseBrokerUsage(address);
            return address;
        }
        else
        {
            return null;
        }
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
