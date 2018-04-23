package io.zeebe.broker.clustering.orchestration.state;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.transport.*;
import io.zeebe.util.sched.ActorControl;

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

    public abstract void execute(ClientTransport serverOutput, Function<SocketAddress, SocketAddress> addressSupplier, LogStreamWriter logStreamWriter);
}
