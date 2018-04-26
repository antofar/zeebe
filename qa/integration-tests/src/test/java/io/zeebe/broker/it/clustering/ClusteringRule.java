/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.clustering;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.ClientRule;
import io.zeebe.client.ZeebeClient;
import io.zeebe.client.clustering.impl.BrokerPartitionState;
import io.zeebe.client.clustering.impl.TopologyBroker;
import io.zeebe.client.event.Event;
import io.zeebe.client.topic.Topic;
import io.zeebe.client.topic.Topics;
import io.zeebe.test.util.AutoCloseableRule;
import io.zeebe.transport.SocketAddress;
import org.junit.rules.ExternalResource;

import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static io.zeebe.test.util.TestUtil.doRepeatedly;
import static org.assertj.core.api.Assertions.assertThat;

public class ClusteringRule extends ExternalResource
{
    public static final int DEFAULT_REPLICATION_FACTOR = 1;

    public static final String BROKER_1_TOML = "zeebe.cluster.1.cfg.toml";
    public static final SocketAddress BROKER_1_CLIENT_ADDRESS = new SocketAddress("localhost", 51015);

    public static final String BROKER_2_TOML = "zeebe.cluster.2.cfg.toml";
    public static final SocketAddress BROKER_2_CLIENT_ADDRESS = new SocketAddress("localhost", 41015);

    public static final String BROKER_3_TOML = "zeebe.cluster.3.cfg.toml";
    public static final SocketAddress BROKER_3_CLIENT_ADDRESS = new SocketAddress("localhost", 31015);

    private SocketAddress[] brokerAddresses = new SocketAddress[]{BROKER_1_CLIENT_ADDRESS, BROKER_2_CLIENT_ADDRESS, BROKER_3_CLIENT_ADDRESS};
    private String[] brokerConfigs = new String[]{BROKER_1_TOML, BROKER_2_TOML, BROKER_3_TOML};
    private final int spreadCount = brokerAddresses.length * 2;

    // rules
    private final AutoCloseableRule autoCloseableRule;
    private final ClientRule clientRule;

    // interal
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;
    private ZeebeClient zeebeClient;
    protected final Map<SocketAddress, Broker> brokers = new HashMap<>();

    public ClusteringRule(AutoCloseableRule autoCloseableRule, ClientRule clientRule, SocketAddress[] brokerAddresses, String[] brokerConfigs)
    {
        this(autoCloseableRule, clientRule);
        this.brokerAddresses = brokerAddresses;
        this.brokerConfigs = brokerConfigs;
    }

    public ClusteringRule(AutoCloseableRule autoCloseableRule, ClientRule clientRule)
    {
        this.autoCloseableRule = autoCloseableRule;
        this.clientRule = clientRule;
    }

    @Override
    protected void before()
    {
        zeebeClient = clientRule.getClient();

        for (int i = 0; i < brokerConfigs.length; i++)
        {
            brokers.put(brokerAddresses[i], startBroker(brokerConfigs[i]));
        }

        waitForInternalSystemAndReplicationFactor(1);

        waitUntilBrokersInTopology(3);
    }

    private void waitUntilBrokersInTopology(int size)
    {
        waitForSpreading(() -> {
            doRepeatedly(() -> zeebeClient.requestTopology().execute().getBrokers())
                .until(topologyBrokers -> topologyBrokers.size() == size);
        });
    }

    private void waitForInternalSystemAndReplicationFactor(int replicationFactor)
    {
        waitForTopicPartitionReplicationFactor("internal-system", 1, replicationFactor);
    }

    /**
     * Returns the current leader for the given partition.
     *
     * @param partition
     * @return
     */
    public TopologyBroker getLeaderForPartition(int partition)
    {
        return
            doRepeatedly(() -> {
                final List<TopologyBroker> brokers = zeebeClient.requestTopology().execute().getBrokers();
                return extractPartitionLeader(brokers, partition);
            })
                .until(Optional::isPresent)
                .get();
    }

    private Optional<TopologyBroker> extractPartitionLeader(List<TopologyBroker> topologyBrokers, int partition)
    {
        return topologyBrokers.stream()
            .filter(b -> b.getPartitions()
                    .stream()
                    .anyMatch(p -> p.getPartitionId() == partition && p.isLeader())
            )
            .findFirst();
    }

    /**
     * Creates a topic with the given partition count in the cluster.
     *
     * This method returns to the user, if the topic and the partitions are created
     * and the replication factor was reached for each partition.
     * Besides that the topic request needs to be return the created topic.
     *
     * The replication factor is per default {@link #DEFAULT_REPLICATION_FACTOR}, but can be modified with
     * {@link #setReplicationFactor(int)}.
     *
     * @param topicName
     * @param partitionCount
     * @return
     */
    public Topic createTopic(String topicName, int partitionCount)
    {
       return createTopic(topicName, partitionCount, 1);
    }

    public Topic createTopic(String topicName, int partitionCount, int replicationFactor)
    {
        final Event topicEvent = zeebeClient.topics()
                                         .create(topicName, partitionCount, replicationFactor)
                                         .execute();
        assertThat(topicEvent.getState()).isEqualTo("CREATING");

        waitForTopicPartitionReplicationFactor(topicName, partitionCount, replicationFactor);

        return waitForSpreading(() -> waitForTopicAvailability(topicName));
    }

    private boolean hasPartitionsWithReplicationFactor(List<TopologyBroker> brokers, String topicName, int partitionCount, int replicationFactor)
    {
        final Map<Integer, List<BrokerPartitionState>> brokersPerPartition = brokers.stream()
                .flatMap(b -> b.getPartitions().stream())
                .filter(p -> topicName.equals(p.getTopicName()))
                .collect(Collectors.groupingBy(p -> p.getPartitionId()));

        if (brokersPerPartition.size() == partitionCount)
        {
            return brokersPerPartition
                    .values()
                    .stream()
                    .allMatch(m -> m.size() >= replicationFactor);
        }
        else
        {
            return false;
        }
    }

    private void waitForTopicPartitionReplicationFactor(String topicName, int partitionCount, int replicationFactor)
    {
        waitForSpreading(() -> {
            doRepeatedly(() -> zeebeClient.requestTopology().execute().getBrokers())
                .until(topologyBrokers -> hasPartitionsWithReplicationFactor(topologyBrokers, topicName, partitionCount, replicationFactor));
        });
    }

    private Topic waitForTopicAvailability(String topicName)
    {
        return doRepeatedly(() -> {
            final Topics topics = zeebeClient.topics().getTopics().execute();
            return topics.getTopics().stream().filter(topic -> topicName.equals(topic.getName())).findAny();
        })
            .until(Optional::isPresent)
            .get();
    }

    private Broker startBroker(String configFile)
    {
        final InputStream config = this.getClass().getClassLoader().getResourceAsStream(configFile);
        final Broker broker = new Broker(config);
        autoCloseableRule.manage(broker);

        return broker;
    }

    /**
     * Restarts broker, if the broker is still running it will be closed before.
     *
     * Returns to the user if the broker is back in the cluster.
     *
     * @param socketAddress
     * @return
     */
    public void restartBroker(SocketAddress socketAddress)
    {
        final Broker broker = brokers.get(socketAddress);
        if (broker != null)
        {
            stopBroker(socketAddress);
        }

        for (int i = 0; i < brokerAddresses.length; i++)
        {
            if (brokerAddresses[i].equals(socketAddress))
            {
                brokers.put(socketAddress, startBroker(brokerConfigs[i]));

                waitForInternalSystemAndReplicationFactor(replicationFactor);
                break;
            }
        }

        waitUntilBrokerIsAddedToTopology(socketAddress);
    }

    private void waitUntilBrokerIsAddedToTopology(SocketAddress socketAddress)
    {
        waitForSpreading(() -> {
            doRepeatedly(() -> zeebeClient.requestTopology().execute().getBrokers())
                .until(topologyBrokers -> topologyBrokers.stream().anyMatch(topologyBroker -> topologyBroker.getSocketAddress().equals(socketAddress)));
        });
    }

    /**
     * Returns for a given broker the leading partition id's.
     *
     * @param socketAddress
     * @return
     */
    public List<Integer> getBrokersLeadingPartitions(SocketAddress socketAddress)
    {
        return zeebeClient.requestTopology()
                          .execute()
                          .getBrokers()
                          .stream()
                          .filter(broker -> broker.getSocketAddress().equals(socketAddress))
                          .flatMap(broker -> broker.getPartitions().stream())
                          .filter(BrokerPartitionState::isLeader)
                          .map(BrokerPartitionState::getPartitionId)
                          .collect(Collectors.toList());
    }

    /**
     * Returns the list of available brokers in a cluster.
     * @return
     */
    public List<SocketAddress> getBrokersInCluster()
    {
        return zeebeClient.requestTopology()
                          .execute()
                          .getBrokers()
                          .stream()
                          .map(TopologyBroker::getSocketAddress)
                          .collect(Collectors.toList());

    }

    public SocketAddress[] getOtherBrokers(SocketAddress address)
    {
        final List<SocketAddress> collect = brokers.keySet()
            .stream()
            .filter((s) -> !s.equals(address))
            .collect(Collectors.toList());
        return collect.toArray(new SocketAddress[collect.size()]);
    }

    /**
     * Returns the count of partition leaders for a given topic.
     *
     * @param topic
     * @return
     */
    public long getPartitionLeaderCountForTopic(String topic)
    {

        return zeebeClient.requestTopology()
                          .execute()
                          .getBrokers()
                          .stream()
                          .flatMap(broker -> broker.getPartitions().stream())
                          .filter(p -> p.getTopicName().equals(topic) && p.isLeader())
                          .count();
    }

    /**
     * Stops broker with the given socket address.
     *
     * Returns to the user if the broker was stopped and new leader for the partitions
     * are chosen.
     *
     * @param socketAddress
     */
    public void stopBroker(SocketAddress socketAddress)
    {
        final List<Integer> brokersLeadingPartitions = getBrokersLeadingPartitions(socketAddress);

        brokers.remove(socketAddress).close();

        waitForNewLeaderOfPartitions(brokersLeadingPartitions, socketAddress);
        waitUntilBrokerIsRemovedFromTopology(socketAddress);
    }

    private void waitUntilBrokerIsRemovedFromTopology(SocketAddress socketAddress)
    {
        waitForSpreading(() -> {
            doRepeatedly(() -> zeebeClient.requestTopology().execute().getBrokers())
                .until(topologyBrokers -> topologyBrokers.stream().noneMatch(topologyBroker -> topologyBroker.getSocketAddress().equals(socketAddress)));
        });
    }

    private void waitForNewLeaderOfPartitions(List<Integer> partitions, SocketAddress oldLeader)
    {
        waitForSpreading(() -> {
            doRepeatedly(() -> zeebeClient.requestTopology().execute().getBrokers())
                .until(topologyBrokers ->
                    topologyBrokers != null && topologyBrokers.stream()
                        .filter(broker -> !broker.getSocketAddress().equals(oldLeader))
                        .flatMap(broker -> broker.getPartitions().stream())
                        .filter(BrokerPartitionState::isLeader)
                        .map(BrokerPartitionState::getPartitionId)
                        .collect(Collectors.toSet())
                        .containsAll(partitions));
        });
    }


    private void waitForSpreading(Runnable r)
    {
        for (int i = 0; i < spreadCount; i++)
        {
            r.run();

            // retry to make sure
        }
    }

    private <V> V waitForSpreading(Callable<V> callable)
    {
        V value = null;
        for (int i = 0; i < spreadCount; i++)
        {
            try
            {
                value = callable.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }

            // retry to make sure
        }
        return value;
    }

    public void setReplicationFactor(int replicationFactor)
    {
        this.replicationFactor = replicationFactor;
    }
}
