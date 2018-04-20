package io.zeebe.broker.clustering.orchestration.state;

import java.util.Objects;
import java.util.function.Supplier;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.CreatePartitionRequest;
import io.zeebe.broker.clustering.orchestration.generation.IdGenerator;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.transport.TransportMessage;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;

public class CreatePartitionCommand extends OrchestrationCommand
{
    private final ActorControl actor;
    private final IdGenerator idGenerator;

    public CreatePartitionCommand(final String topicName, final int replicationFactor, final int count, final ActorControl actor, final IdGenerator idGenerator)
    {
        super(topicName, -1, replicationFactor, count);
        this.actor = actor;
        this.idGenerator = idGenerator;
    }

    @Override
    public void execute(final ClientTransport clientTransport, final Supplier<SocketAddress> addressSupplier)
    {
        Loggers.CLUSTERING_LOGGER.debug("Executing orchestration command: {}", this);
        for (int i = 0; i < count; i++)
        {
            actor.runOnCompletion(idGenerator.nextId(), (partitionId, throwable) -> {
                if (throwable != null)
                {
                    final SocketAddress socketAddress = addressSupplier.get();
                    final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(socketAddress);

                    remoteAddresses.add(remoteAddress);

                    final CreatePartitionRequest request = new CreatePartitionRequest().topicName(BufferUtil.wrapString(topicName))
                                                                                       .partitionId(partitionId)
                                                                                       .replicationFactor(replicationFactor);

                    final TransportMessage message = new TransportMessage().remoteAddress(remoteAddress).writer(request);
                    // TODO: think about error handling, maybe not
                    clientTransport.getOutput().sendMessage(message);
                }
                else
                {
                    // TODO: loop or not thats the question
                }
            });
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
        return replicationFactor == that.replicationFactor && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(topicName, replicationFactor);
    }

    @Override
    public String toString()
    {
        return "CreatePartitionCommand{" + "topicName='" + topicName + '\'' + ", partitionId=" + partitionId + ", replicationFactor=" + replicationFactor
            + ", count=" + count + '}';
    }
}
