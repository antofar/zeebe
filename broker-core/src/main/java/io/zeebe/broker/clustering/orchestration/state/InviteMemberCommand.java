package io.zeebe.broker.clustering.orchestration.state;

import java.util.Collections;
import java.util.Objects;
import java.util.function.Supplier;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.InvitationRequest;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.TransportMessage;
import io.zeebe.util.buffer.BufferUtil;

public class InviteMemberCommand extends OrchestrationCommand
{
    private final SocketAddress leader;

    public InviteMemberCommand(final String topicName, final int partitionId, final int replicationFactor, SocketAddress leader, final int count)
    {
        super(topicName, partitionId, replicationFactor, count);
        this.leader = leader;
    }

    @Override
    public void execute(ClientTransport clientTransport, Supplier<SocketAddress> addressSupplier)
    {
        Loggers.CLUSTERING_LOGGER.debug("Executing orchestration command: {}", this);
        for (int i = 0; i < count; i++)
        {
            final SocketAddress socketAddress = addressSupplier.get();
            final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(socketAddress);

            remoteAddresses.add(remoteAddress);

            final InvitationRequest request = new InvitationRequest()
                .topicName(BufferUtil.wrapString(topicName))
                .partitionId(partitionId)
                .replicationFactor(replicationFactor)
                .members(Collections.singletonList(leader));

            final TransportMessage message = new TransportMessage().remoteAddress(remoteAddress).writer(request);
            // TODO: think about error handling, maybe not
            clientTransport.getOutput().sendMessage(message);
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
