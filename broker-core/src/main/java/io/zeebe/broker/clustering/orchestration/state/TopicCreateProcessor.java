package io.zeebe.broker.clustering.orchestration.state;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import java.util.function.Consumer;
import java.util.function.Predicate;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class TopicCreateProcessor implements TypedEventProcessor<TopicEvent>
{

    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;


    private final Predicate<DirectBuffer> topicExists;
    private final Consumer<TopicInfo> updateTopicState;

    public TopicCreateProcessor(final Predicate<DirectBuffer> topicExists, final Consumer<TopicInfo> updateTopicState)
    {
        this.topicExists = topicExists;
        this.updateTopicState = updateTopicState;
    }

    @Override
    public void processEvent(final TypedEvent<TopicEvent> event)
    {
        final TopicEvent topicEvent = event.getValue();

        final DirectBuffer topicName = topicEvent.getName();
        if (topicExists.test(topicName))
        {
            LOG.warn("Rejecting topic {} creation as a topic with the same name already exists", bufferAsString(topicName));
            topicEvent.setState(TopicState.CREATE_REJECTED);
        }
        else
        {
            LOG.info("Creating topic {} with partition count {} and replication factor {}", bufferAsString(topicName), topicEvent.getPartitions(), topicEvent.getReplicationFactor());
            topicEvent.setState(TopicState.CREATING);
        }
    }

    @Override
    public boolean executeSideEffects(final TypedEvent<TopicEvent> event, final TypedResponseWriter responseWriter)
    {
        return responseWriter.write(event);
    }

    @Override
    public long writeEvent(final TypedEvent<TopicEvent> event, final TypedStreamWriter writer)
    {
        return writer.writeFollowupEvent(event.getKey(), event.getValue());
    }

    @Override
    public void updateState(final TypedEvent<TopicEvent> event)
    {
        final TopicEvent topicEvent = event.getValue();
        if (topicEvent.getState() == TopicState.CREATING)
        {
            final TopicInfo topicInfo = new TopicInfo(event.getPosition(), topicEvent);
            updateTopicState.accept(topicInfo);
        }
    }
}
