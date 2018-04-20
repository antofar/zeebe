/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering.base.partitions;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.raft.config.RaftPersistentConfiguration;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.logstreams.LogStreams;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.raft.Raft;
import io.zeebe.raft.RaftConfiguration;
import io.zeebe.raft.RaftMath;
import io.zeebe.raft.RaftStateListener;
import io.zeebe.raft.controller.MemberReplicateLogController;
import io.zeebe.raft.state.RaftState;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.channel.OneToOneRingBufferChannel;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.slf4j.Logger;

import java.util.Collection;

import static io.zeebe.broker.clustering.base.ClusterBaseLayerServiceNames.*;
import static io.zeebe.raft.RaftServiceNames.*;

/**
 * Service used to install the necessary services for creating a partition, namely logstream and raft.
 * Also listens to raft state changes (Leader, Follower) and installs the corresponding {@link Partition} service(s)
 * into the broker for other components (like client api or stream processing) to attach to.
 */
public class PartitionInstallService implements Service<Void>, RaftStateListener
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    private final Injector<NodeInfo> localNodeInjector = new Injector<>();
    private final Injector<ClientTransport> clientTransportInjector = new Injector<>();
    private final RaftPersistentConfiguration configuration;
    private final PartitionInfo partitionInfo;
    private final boolean isInternalSystemPartition;

    private ServiceStartContext startContext;
    private ServiceName<LogStream> logStreamServiceName;

    public PartitionInstallService(RaftPersistentConfiguration configuration, boolean isInternalSystemPartition)
    {
        this.configuration = configuration;
        this.isInternalSystemPartition = isInternalSystemPartition;
        this.partitionInfo = new PartitionInfo(configuration.getTopicName(), configuration.getPartitionId(), configuration.getReplicationFactor());
    }

    @Override
    public Void get()
    {
        return null;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        this.startContext = startContext;

        final NodeInfo localNode = localNodeInjector.getValue();
        final ClientTransport clientTransport = clientTransportInjector.getValue();

        final DirectBuffer topicName = configuration.getTopicName();
        final String topicNameString = BufferUtil.bufferAsString(topicName);
        final int partitionId = configuration.getPartitionId();
        final String logName = String.format("%s-%d", topicNameString, partitionId);

        final ServiceName<Void> raftInstallServiceName = raftInstallServiceName(topicNameString, partitionId);
        final ServiceName<Raft> raftServiceName = raftServiceName(logName);

        final CompositeServiceBuilder partitionInstall = startContext.createComposite(raftInstallServiceName);

        logStreamServiceName = LogStreams.createFsLogStream(topicName, partitionId)
            .logDirectory(configuration.getLogDirectory())
            .logName(logName)
            .buildWith(partitionInstall);

        final OneToOneRingBufferChannel messageBuffer = new OneToOneRingBufferChannel(new UnsafeBuffer(new byte[(MemberReplicateLogController.REMOTE_BUFFER_SIZE) + RingBufferDescriptor.TRAILER_LENGTH]));

        final Raft raftService = new Raft(logName,
            new RaftConfiguration(),
            localNode.getReplicationPort(),
            clientTransport,
            configuration,
            messageBuffer,
            this);

        raftService.addMembersWhenJoined(configuration.getMembers());

        partitionInstall.createService(raftServiceName, raftService)
            .dependency(logStreamServiceName, raftService.getLogStreamInjector())
            .install();

        partitionInstall.install();
    }

    @Override
    public void onMembersChanged(Raft raft, Collection<SocketAddress> addresses)
    {
        if (raft.getState() == RaftState.LEADER)
        {
            installLeaderPartition(raft);
        }
    }

    @Override
    public void onStateChange(Raft raft, RaftState raftState)
    {
        switch (raftState)
        {
            case FOLLOWER:
                installFollowerPartition(raft);
                break;

            case LEADER:
                installLeaderPartition(raft);
                break;

            case CANDIDATE:
                // do nothing
                break;
        }
    }

    private void installFollowerPartition(Raft raft)
    {
        final Partition partition = new Partition(partitionInfo, RaftState.FOLLOWER);
        final ServiceName<Partition> partitionServiceName = followerPartitionServiceName(raft.getName());

        startContext.createService(partitionServiceName, partition)
            .dependency(followerServiceName(raft.getName(), raft.getTerm()))
            .dependency(logStreamServiceName, partition.getLogStreamInjector())
            .group(isInternalSystemPartition ? FOLLOWER_PARTITION_SYSTEM_GROUP_NAME : FOLLOWER_PARTITION_GROUP_NAME)
            .install();
    }

    private void installLeaderPartition(Raft raft)
    {
        final ServiceName<Partition> partitionServiceName = leaderPartitionServiceName(raft.getName());

        final int raftMemberSize = raft.getMemberSize() + 1; // raft does not count itself as member
        final int replicationFactor = partitionInfo.getReplicationFactor();

        final int requiredQuorum = RaftMath.getRequiredQuorum(replicationFactor);
        final int currentQuorum = raft.requiredQuorum();

        if (startContext.hasService(partitionServiceName))
        {
            if (currentQuorum < requiredQuorum)
            {
                LOG.debug("Removing partition service for {}. Replication factor not reached, got {}/{}.", partitionInfo, raftMemberSize, replicationFactor);

                // TODO: ensure data appended after quorum lost and before partition removed
                // does not get committed
                startContext.removeService(partitionServiceName);
            }
        }
        else
        {
            if (currentQuorum >= requiredQuorum)
            {
                LOG.debug("Installing partition service for {}. Replication factor reached, got {}/{}.", partitionInfo, raftMemberSize, replicationFactor);

                final Partition partition = new Partition(partitionInfo, RaftState.LEADER);

                startContext.createService(partitionServiceName, partition)
                    .dependency(leaderInitialEventCommittedServiceName(raft.getName(), raft.getTerm()))
                    .dependency(logStreamServiceName, partition.getLogStreamInjector())
                    .group(isInternalSystemPartition ? LEADER_PARTITION_SYSTEM_GROUP_NAME : LEADER_PARTITION_GROUP_NAME)
                    .install();
            }
            else
            {
                LOG.debug("Not installing partition service for {}. Replication factor not reached, got {}/{}.", partitionInfo, raftMemberSize, replicationFactor);
            }
        }
    }

    public Injector<ClientTransport> getClientTransportInjector()
    {
        return clientTransportInjector;
    }

    public Injector<NodeInfo> getLocalNodeInjector()
    {
        return localNodeInjector;
    }
}
