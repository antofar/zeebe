package io.zeebe.broker.clustering.orchestration.id;

import static io.zeebe.broker.logstreams.processor.StreamProcessorIds.SYSTEM_ID_PROCESSOR_ID;

import java.util.ArrayDeque;
import java.util.Queue;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.logstreams.log.LogStreamWriterImpl;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.servicecontainer.*;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.future.CompletableActorFuture;
import org.slf4j.Logger;

public class IdGenerator implements TypedEventProcessor<IdEvent>, Service<IdGenerator>
{

    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();
    private final Injector<Partition> partitionInjector = new Injector<>();

    private final Queue<ActorFuture<Integer>> pendingFutures = new ArrayDeque<>();
    private final IdEvent idEvent = new IdEvent();

    private int committedId = 0;
    private int nextIdToWrite = Protocol.SYSTEM_PARTITION + 1;

    private ActorControl actor;

    private LogStreamWriterImpl logStreamWriter;

    @Override
    public void onOpen(final TypedStreamProcessor streamProcessor)
    {
        actor = streamProcessor.getActor();
    }

    @Override
    public boolean executeSideEffects(final TypedEvent<IdEvent> event, final TypedResponseWriter responseWriter)
    {
        // complete pending futures
        final IdEvent value = event.getValue();
        final ActorFuture<Integer> pendingIdFuture = pendingFutures.poll();
        if (pendingIdFuture != null)
        {
            LOG.debug("Id generated {}", idEvent);
            pendingIdFuture.complete(value.getId());
        }
        else
        {
            LOG.warn("No pending id request found, ignoring id event {}", idEvent);
        }
        return true;
    }

    @Override
    public void updateState(final TypedEvent<IdEvent> event)
    {
        committedId = event.getValue().getId();
    }

    @Override
    public void start(final ServiceStartContext startContext)
    {
        final ServerTransport clientApiTransport = clientApiTransportInjector.getValue();
        final StreamProcessorServiceFactory streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
        final Partition leaderSystemPartition = partitionInjector.getValue();

        logStreamWriter = new LogStreamWriterImpl();

        final TypedStreamEnvironment typedStreamEnvironment = new TypedStreamEnvironment(leaderSystemPartition.getLogStream(), clientApiTransport.getOutput());

        final TypedStreamProcessor streamProcessor = typedStreamEnvironment.newStreamProcessor()
                                                                           .onEvent(EventType.ID_EVENT, IdEventState.GENERATED,  this)
                                                                           .build();

        logStreamWriter.wrap(leaderSystemPartition.getLogStream());

        streamProcessorServiceFactory.createService(leaderSystemPartition, partitionInjector.getInjectedServiceName())
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
            if (nextIdToWrite <= committedId)
            {
                nextIdToWrite = committedId + 1;
            }

            final BrokerEventMetadata metadata = new BrokerEventMetadata();
            metadata.eventType(EventType.ID_EVENT);

            idEvent.reset();
            idEvent.setState(IdEventState.GENERATED);
            idEvent.setId(nextIdToWrite);

            final long position = logStreamWriter
                .valueWriter(idEvent)
                .metadataWriter(metadata)
                .positionAsKey()
                .tryWrite();

            if (position < 0)
            {
                nextId.completeExceptionally(new RuntimeException("Unable to write id event."));
            }
            else
            {
                pendingFutures.add(nextId);
                nextIdToWrite++;
            }
        });

        return nextId;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }

    public Injector<Partition> getPartitionInjector()
    {
        return partitionInjector;
    }
}
