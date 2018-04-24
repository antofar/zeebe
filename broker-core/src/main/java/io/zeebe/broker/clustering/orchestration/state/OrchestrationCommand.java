package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.SocketAddress;
import io.zeebe.util.sched.ActorControl;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public abstract class OrchestrationCommand
{

    protected final ActorControl actor;
    // TODO: direct buffer please
    protected String topicName;
    protected int partitionId;
    protected int replicationFactor;

    protected int count;

    protected List<RemoteAddress> remoteAddresses;

    public OrchestrationCommand(final String topicName, final int partitionId, final int replicationFactor, final int count, final ActorControl actor)
    {
        this.topicName = topicName;
        this.partitionId = partitionId;
        this.replicationFactor = replicationFactor;
        this.count = count;
        this.remoteAddresses = new ArrayList<>();
        this.actor = actor;
    }

    public List<RemoteAddress> getRemoteAddresses()
    {
        return remoteAddresses;
    }

    public abstract void execute(ClientTransport serverOutput, BiFunction<SocketAddress, Integer, List<SocketAddress>> addressSupplier, LogStreamWriter logStreamWriter);
}
