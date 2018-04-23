package io.zeebe.broker.clustering.orchestration.state;

import java.util.Objects;
import java.util.function.Supplier;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.api.CreatePartitionRequest;
import io.zeebe.broker.clustering.orchestration.generation.IdEventState;
import io.zeebe.broker.clustering.orchestration.generation.IdGenerator;
import io.zeebe.broker.system.log.PartitionEvent;
import io.zeebe.broker.system.log.PartitionState;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;
import io.zeebe.logstreams.log.LogStreamWriter;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.impl.BrokerEventMetadata;
import io.zeebe.transport.*;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;

public class CreatePartitionCommand extends OrchestrationCommand
{
    private final ActorControl actor;
    private final IdGenerator idGenerator;

    private int partitions;
    private int completedPartitions;

    public CreatePartitionCommand(final String topicName, final int replicationFactor, final int count, final ActorControl actor, final IdGenerator idGenerator, final int partitions)
    {
        super(topicName, -1, replicationFactor, count);
        this.actor = actor;
        this.idGenerator = idGenerator;
        this.partitions = partitions;
    }


    @Override
    public void execute(final ClientTransport clientTransport, final Supplier<SocketAddress> addressSupplier, LogStreamWriter logStreamWriter)
    {
        Loggers.CLUSTERING_LOGGER.debug("Executing orchestration command: {}", this);
        for (int i = 0; i < count; i++)
        {
            actor.runOnCompletion(idGenerator.nextId(), (partitionId, throwable) ->
            {
                if (throwable == null)
                {
                    final SocketAddress socketAddress = addressSupplier.get();
                    if (socketAddress != null)
                    {
                        final RemoteAddress remoteAddress = clientTransport.registerRemoteAddress(socketAddress);

                        remoteAddresses.add(remoteAddress);

                        Loggers.CLUSTERING_LOGGER.debug("Send create partition request to {} with partition id {}", socketAddress, partitionId);

                        final DirectBuffer topicNameBuffer = BufferUtil.wrapString(this.topicName);
                        final CreatePartitionRequest request = new CreatePartitionRequest().topicName(topicNameBuffer)
                                                                                           .partitionId(partitionId)
                                                                                           .replicationFactor(replicationFactor);

                        //                    final TransportMessage message = new TransportMessage().remoteAddress(remoteAddress).writer(request);
                        // TODO: think about error handling, maybe not
                        //                    clientTransport.getOutput().sendMessage(message);

                        // TODO: message or request?
                        final ActorFuture<ClientResponse> responseFuture = clientTransport.getOutput().sendRequest(remoteAddress, request);

                        actor.runOnCompletion(responseFuture, (createPartitionResponse, createPartitionError) ->
                        {
                            if (createPartitionError != null)
                            {
                                Loggers.CLUSTERING_LOGGER.error("Error while creating partition {}", partitionId, createPartitionError);
                            }
                            else
                            {
                                // TODO: do not do this HELP!!!
                                BrokerEventMetadata metadata = new BrokerEventMetadata();
                                metadata.eventType(EventType.PARTITION_EVENT);

                                final PartitionEvent event = new PartitionEvent();
                                event.setParitionId(partitionId);
                                event.setReplicationFactor(replicationFactor);
                                event.setTopicName(BufferUtil.wrapString(topicName));
                                event.setCreator(socketAddress.getHostBuffer(), socketAddress.port());
                                event.setState(PartitionState.CREATED);

                                long position;
                                do {
                                    position = logStreamWriter
                                        .valueWriter(event)
                                        .metadataWriter(metadata)
                                        .positionAsKey()
                                        .tryWrite();
                                    Loggers.CLUSTERING_LOGGER.error("HELP: {}", position);
                                } while (position < 0);

                                completedPartitions++;
                                Loggers.CLUSTERING_LOGGER.debug("Partition {} creation successful.", partitionId);

                                if (completedPartitions == count)
                                {
                                    // TODO: THIS IS EVEN WORSE
                                    metadata = new BrokerEventMetadata();
                                    metadata.eventType(EventType.TOPIC_EVENT);

                                    final TopicEvent topicEvent = new TopicEvent();
                                    topicEvent.setState(TopicState.CREATED);
                                    topicEvent.setName(BufferUtil.wrapString(topicName));
                                    topicEvent.setPartitions(partitions);
                                    topicEvent.setReplicationFactor(replicationFactor);

                                    do {
                                        position = logStreamWriter
                                            .valueWriter(topicEvent)
                                            .metadataWriter(metadata)
                                            .positionAsKey()
                                            .tryWrite();
                                        Loggers.CLUSTERING_LOGGER.error("HELP2: {}", position);
                                    } while (position < 0);

                                    Loggers.CLUSTERING_LOGGER.debug("Topic {} with {} partitions creation successful.", topicName, partitions);
                                }
                            }
                        });
                    }
                    else
                    {
                        Loggers.CLUSTERING_LOGGER.warn("Address supplier is unable to provide next socket address");
                    }
                }
                else
                {

                    Loggers.CLUSTERING_LOGGER.debug("Error in generating partition id {}.", partitionId, throwable);
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
        return "CreatePartitionCommand{" + "topicName='" + topicName + '\'' + ", partitionId=" + partitionId + ", replicationFactor=" + replicationFactor + ", count=" + count + '}';
    }
}
