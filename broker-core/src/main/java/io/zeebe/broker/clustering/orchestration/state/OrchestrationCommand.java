package io.zeebe.broker.clustering.orchestration.state;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerOutput;
import io.zeebe.transport.SocketAddress;
import org.agrona.DirectBuffer;

public abstract class OrchestrationCommand
{

    // TODO: direct buffer please
    protected String topicName;
    protected int partitionId;
    protected int replicationFactor;

    protected int count;

    protected List<RemoteAddress> remoteAddresses;

    public OrchestrationCommand(final String topicName, final int partitionId, final int replicationFactor, final int count)
    {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
        this.count = count;
        this.remoteAddresses = new ArrayList<>();
    }

    public List<RemoteAddress> getRemoteAddresses()
    {
        return remoteAddresses;
    }

    public abstract void execute(final ClientTransport serverOutput, final Supplier<SocketAddress> addressSupplier);
}
