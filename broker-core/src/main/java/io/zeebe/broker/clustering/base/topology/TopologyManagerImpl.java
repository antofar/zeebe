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
package io.zeebe.broker.clustering.base.topology;

import static io.zeebe.broker.clustering.base.gossip.GossipCustomEventEncoding.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.gossip.*;
import io.zeebe.gossip.dissemination.GossipSyncRequest;
import io.zeebe.gossip.membership.Member;
import io.zeebe.raft.state.RaftState;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.LogUtil;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.*;
import org.slf4j.Logger;

public class TopologyManagerImpl extends Actor implements TopologyManager
{
    private static final Logger LOG = Loggers.CLUSTERING_LOGGER;

    public static final DirectBuffer CONTACT_POINTS_EVENT_TYPE = BufferUtil.wrapString("contact_points");
    public static final DirectBuffer PARTITIONS_EVENT_TYPE = BufferUtil.wrapString("partitions");

    private final MembershipListener membershipListner = new MembershipListener();
    private final ContactPointsChangeListener contactPointsChangeListener = new ContactPointsChangeListener();
    private final ParitionChangeListener paritionChangeListener = new ParitionChangeListener();
    private final KnownContactPointsSyncHandler localContactPointsSycHandler = new KnownContactPointsSyncHandler();
    private final KnownPartitionsSyncHandler knownPartitionsSyncHandler = new KnownPartitionsSyncHandler();

    private final Topology topology;
    private final Gossip gossip;

    private List<TopologyMemberListener> topologyMemberListers = new ArrayList<>();
    private List<TopologyPartitionListener> topologyPartitionListers = new ArrayList<>();

    public TopologyManagerImpl(Gossip gossip, NodeInfo localBroker)
    {
        this.gossip = gossip;
        this.topology = new Topology(localBroker);
    }

    @Override
    protected void onActorStarting()
    {
        gossip.addMembershipListener(membershipListner);

        gossip.addCustomEventListener(CONTACT_POINTS_EVENT_TYPE, contactPointsChangeListener);
        gossip.addCustomEventListener(PARTITIONS_EVENT_TYPE, paritionChangeListener);

        gossip.registerSyncRequestHandler(CONTACT_POINTS_EVENT_TYPE, localContactPointsSycHandler);
        gossip.registerSyncRequestHandler(PARTITIONS_EVENT_TYPE, knownPartitionsSyncHandler);

        publishLocalContactPoints();
    }

    @Override
    protected void onActorClosing()
    {
        gossip.removeCustomEventListener(paritionChangeListener);
        gossip.removeCustomEventListener(contactPointsChangeListener);

        // remove gossip sync handlers?
    }

    public void onPartitionStarted(Partition partition)
    {
        actor.run(() ->
        {
            final NodeInfo memberInfo = topology.getLocal();
            final PartitionInfo partitionInfo = partition.getInfo();

            updatePartition(partitionInfo.getPartitionId(),
                partitionInfo.getTopicName(),
                partitionInfo.getReplicationFactor(),
                memberInfo,
                partition.getState());

            publishLocalPartitions();
        });
    }

    public void updatePartition(int partitionId, DirectBuffer topicBuffer, int replicationFactor, NodeInfo member, RaftState raftState)
    {
        final PartitionInfo updatedPartition = topology.updatePartition(partitionId,
            topicBuffer,
            replicationFactor,
            member,
            raftState);

        notifyPartitionUpdated(updatedPartition);
    }

    public void onPartitionRemoved(Partition partition)
    {
        actor.run(() ->
        {
            final NodeInfo memberInfo = topology.getLocal();

            topology.removePartitionForMember(partition.getInfo().getPartitionId(), memberInfo);

            publishLocalPartitions();
        });
    }

    private class ContactPointsChangeListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress sender, DirectBuffer payload)
        {
            final SocketAddress senderCopy = new SocketAddress(sender);
            final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

            actor.run(() ->
            {
                LOG.trace("Received API event from member {}.", senderCopy);

                int offset = 0;

                final SocketAddress managementApi = new SocketAddress();
                offset = readSocketAddress(offset, payloadCopy, managementApi);

                final SocketAddress clientApi = new SocketAddress();
                offset = readSocketAddress(offset, payloadCopy, clientApi);

                final SocketAddress replicationApi = new SocketAddress();
                readSocketAddress(offset, payloadCopy, replicationApi);

                final NodeInfo newMember = new NodeInfo(clientApi, managementApi, replicationApi);
                topology.addMember(newMember);
                notifyMemberAdded(newMember);
            });
        }
    }

    private class MembershipListener implements GossipMembershipListener
    {
        @Override
        public void onAdd(Member member)
        {
            // noop; we listen on the availability of contact points, see ContactPointsChangeListener
        }

        @Override
        public void onRemove(Member member)
        {
            final NodeInfo topologyMember = topology.getMemberByAddress(member.getAddress());
            if (topologyMember != null)
            {
                topology.removeMember(topologyMember);
                notifyMemberRemoved(topologyMember);
            }
        }
    }

    private class ParitionChangeListener implements GossipCustomEventListener
    {
        @Override
        public void onEvent(SocketAddress sender, DirectBuffer payload)
        {
            final SocketAddress senderCopy = new SocketAddress(sender);
            final DirectBuffer payloadCopy = BufferUtil.cloneBuffer(payload);

            actor.run(() ->
            {
                LOG.trace("Received raft state change event for member {}", senderCopy);

                final NodeInfo member = topology.getMemberByAddress(senderCopy);

                if (member != null)
                {
                    readPartitions(payloadCopy, 0, member, TopologyManagerImpl.this);
                }
                else
                {
                    LOG.trace("Received raft state change event for unknown member {}", senderCopy);
                }
            });
        }
    }

    private class KnownContactPointsSyncHandler implements GossipSyncRequestHandler
    {
        private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

        @Override
        public ActorFuture<Void> onSyncRequest(GossipSyncRequest request)
        {
            return actor.call(() ->
            {
                LOG.trace("Got API sync request");

                for (NodeInfo member : topology.getMembers())
                {
                    final int length = writeSockedAddresses(member, writeBuffer, 0);
                    request.addPayload(member.getApiPort(), writeBuffer, 0, length);
                }

                LOG.trace("Send API sync response.");
            });
        }
    }

    private class KnownPartitionsSyncHandler implements GossipSyncRequestHandler
    {
        private final ExpandableArrayBuffer writeBuffer = new ExpandableArrayBuffer();

        @Override
        public ActorFuture<Void> onSyncRequest(GossipSyncRequest request)
        {
            return actor.call(() ->
            {
                LOG.trace("Got RAFT state sync request.");

                for (NodeInfo member : topology.getMembers())
                {
                    final int length = writeTopology(topology, writeBuffer, 0);
                    request.addPayload(member.getApiPort(), writeBuffer, 0, length);
                }

                LOG.trace("Send RAFT state sync response.");
            });
        }
    }

    private void publishLocalContactPoints()
    {
        final MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer();
        final int eventLength = writeSockedAddresses(topology.getLocal(), eventBuffer, 0);

        gossip.publishEvent(CONTACT_POINTS_EVENT_TYPE, eventBuffer, 0, eventLength);
    }

    private void publishLocalPartitions()
    {
        final MutableDirectBuffer eventBuffer = new ExpandableArrayBuffer();
        final int length = writePartitions(topology.getLocal(), eventBuffer, 0);

        gossip.publishEvent(PARTITIONS_EVENT_TYPE, eventBuffer, 0, length);
    }

    public ActorFuture<Void> close()
    {
        return actor.close();
    }

    @Override
    public ActorFuture<TopologyDto> getTopologyDto()
    {
        return actor.call(() ->
        {
            return topology.asDto();
        });
    }

    @Override
    public void addTopologyMemberListener(TopologyMemberListener listener)
    {
        actor.run(() ->
        {
            topologyMemberListers.add(listener);

            // notify initially
            topology.getMembers().forEach((m) ->
            {
                LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(m, topology));
            });
        });
    }

    @Override
    public void removeTopologyMemberListener(TopologyMemberListener listener)
    {
        actor.run(() ->
        {
            topologyMemberListers.remove(listener);
        });
    }

    @Override
    public void addTopologyPartitionListener(TopologyPartitionListener listener)
    {
        actor.run(() ->
        {
            topologyPartitionListers.add(listener);

            // notify initially
            topology.getPartitions().forEach((p) ->
            {
                LogUtil.catchAndLog(LOG, () -> listener.onPartitionUpdated(p, topology));
            });
        });
    }

    @Override
    public void removeTopologyPartitionListener(TopologyPartitionListener listener)
    {
        actor.run(() ->
        {
            topologyPartitionListers.remove(listener);
        });
    }

    private void notifyMemberAdded(NodeInfo memberInfo)
    {
        for (TopologyMemberListener listener : topologyMemberListers)
        {
            LogUtil.catchAndLog(LOG, () -> listener.onMemberAdded(memberInfo, topology));
        }
    }

    private void notifyMemberRemoved(NodeInfo memberInfo)
    {
        for (TopologyMemberListener listener : topologyMemberListers)
        {
            LogUtil.catchAndLog(LOG, () -> listener.onMemberRemoved(memberInfo, topology));
        }
    }

    private void notifyPartitionUpdated(PartitionInfo partitionInfo)
    {
        for (TopologyPartitionListener listener : topologyPartitionListers)
        {
            LogUtil.catchAndLog(LOG, () -> listener.onPartitionUpdated(partitionInfo, topology));
        }
    }

    @Override
    public <R> ActorFuture<R> query(Function<Topology, R> query)
    {
        return actor.call(() -> query.apply(topology));
    }
}
