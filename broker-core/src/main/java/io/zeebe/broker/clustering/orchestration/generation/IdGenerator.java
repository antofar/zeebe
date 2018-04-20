package io.zeebe.broker.clustering.orchestration.generation;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;

import java.util.ArrayDeque;
import java.util.Queue;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.SYSTEM_ID_PROCESSOR_ID;

public class IdGenerator implements TypedEventProcessor<IdEvent>, Service<IdGenerator>
{

    private final ServiceGroupReference<Partition> systemLeaderGroupReference = ServiceGroupReference.<Partition>create()
        .onAdd(this::installIdGeneratorProcessor)
        .build();

    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();

    private final Queue<ActorFuture<Integer>> pendingFututes = new ArrayDeque<>();
    private final IdEvent idEvent = new IdEvent();

    private int commitedId = 0;
    private int nextIdToWrite = Protocol.SYSTEM_PARTITION + 1;

    private ActorControl actor;

    private ServerTransport clientApiTransport;
    private StreamProcessorServiceFactory streamProcessorServiceFactory;
    private LogStreamWriterImpl logStreamWriter;

    public enum IdEventState
    {
        NEXT_ID
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        actor = streamProcessor.getActor();
    }

    @Override
    public boolean executeSideEffects(TypedEvent<IdEvent> event, TypedResponseWriter responseWriter)
    {
        // complete pending futures
        final IdEvent value = event.getValue();
        final ActorFuture<Integer> pendingIdFuture = pendingFututes.poll();
        pendingIdFuture.complete(value.getId());
        return true;
    }

    @Override
    public void updateState(TypedEvent<IdEvent> event)
    {
        commitedId = event.getValue().getId();
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        clientApiTransport = clientApiTransportInjector.getValue();
        streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
        logStreamWriter = new LogStreamWriterImpl();
    }

    private void installIdGeneratorProcessor(ServiceName<Partition> leaderPartitionName, Partition leaderSystemPartition)
    {
        final TypedStreamEnvironment typedStreamEnvironment = new TypedStreamEnvironment(leaderSystemPartition.getLogStream(), clientApiTransport.getOutput());

        final TypedStreamProcessor streamProcessor = typedStreamEnvironment.newStreamProcessor()
            .onEvent(EventType.ID_EVENT, IdEventState.NEXT_ID,  this)
            .build();

        logStreamWriter.wrap(leaderSystemPartition.getLogStream());

        streamProcessorServiceFactory.createService(leaderSystemPartition, leaderPartitionName)
            .processor(streamProcessor)
            .processorId(SYSTEM_ID_PROCESSOR_ID)
            .processorName("idGenerator")
            .build();
    }

    @Override
    public IdGenerator get()
    {
        return this;
    }

    public ActorFuture<Integer> nextId()
    {
        final CompletableActorFuture<Integer> nextId = new CompletableActorFuture<>();
        actor.run(() ->
        {
            if (nextIdToWrite <= commitedId)
            {
                nextIdToWrite = commitedId + 1;
            }

            idEvent.setId(nextIdToWrite);

            final long position = logStreamWriter
                .valueWriter(idEvent)
                .positionAsKey()
                .tryWrite();

            if (position < 0)
            {
                nextId.completeExceptionally(new RuntimeException("Unable to write id event."));
            }
            else
            {
                pendingFututes.add(nextId);
                nextIdToWrite++;
            }
        });

        return nextId;
    }

    public ServiceGroupReference<Partition> getSystemLeaderGroupReference()
    {
        return systemLeaderGroupReference;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }
}
