package io.zeebe.broker.clustering.orchestration.state;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.InvitationRequest;
import io.zeebe.broker.clustering.base.topology.NodeInfo;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.transport.*;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;

public class InviteMemberCommand extends OrchestrationCommand
{
    private final NodeInfo leader;

    public InviteMemberCommand(final String topicName, final int partitionId, final int replicationFactor, NodeInfo leader, final int count, final
        ActorControl actor)
    {
        super(topicName, partitionId, replicationFactor, count, actor);
        this.leader = leader;
    }

    @Override
    public void execute(ClientTransport clientTransport, Function<SocketAddress, SocketAddress> addressSupplier, LogStreamWriter logStreamWriter)
    {
        Loggers.CLUSTERING_LOGGER.debug("Executing orchestration command: {}", this);
        for (int i = 0; i < count; i++)
        {
            // TODO: remove hack
            final SocketAddress socketAddress = addressSupplier.apply(leader.getManagementPort());
            if (socketAddress != null)
            {
                final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(socketAddress);

                remoteAddresses.add(remoteAddress);

                final InvitationRequest request = new InvitationRequest()
                    .topicName(BufferUtil.wrapString(topicName))
                    .partitionId(partitionId)
                    .replicationFactor(replicationFactor)
                    .members(Collections.singletonList(leader.getReplicationPort()));

                // TODO: think about error handling, maybe not
                final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

                actor.runOnCompletion(responseFuture, (createPartitionResponse, createPartitionError) ->
                {
                    if (createPartitionError != null)
                    {
                        Loggers.CLUSTERING_LOGGER.error("Error while inviting member {} to partition {}", socketAddress, partitionId, createPartitionError);
                    }
                    else
                    {
                        Loggers.CLUSTERING_LOGGER.debug("Invited member {} to partition {}.", socketAddress, partitionId);
                    }
                });
            }
            else
            {
                Loggers.CLUSTERING_LOGGER.warn("Address supplier is unable to provide next socket address");
            }

        }
    }

    @Override
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        final OrchestrationCommand that = (OrchestrationCommand) o;
        return partitionId == that.partitionId && replicationFactor == that.replicationFactor && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(topicName, partitionId, replicationFactor);
    }

    @Override
    public String toString()
    {
        return "InviteMemberCommand{" + "leader=" + leader + ", topicName='" + topicName + '\'' + ", partitionId=" + partitionId + ", replicationFactor=" + replicationFactor + ", count=" + count + '}';
    }

}
