package io.zeebe.broker.clustering.orchestration.state;

import java.util.function.Consumer;
import java.util.function.Predicate;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventProcessor;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

public class TopicCreatedProcessor implements TypedEventProcessor<TopicEvent>
{
    private final Predicate<DirectBuffer> topicExists;
    private final Consumer<TopicInfo> updateTopicState;

    public TopicCreatedProcessor(final Predicate<DirectBuffer> topicExists, final Consumer<TopicInfo> updateTopicState)
    {
        this.topicExists = topicExists;
        this.updateTopicState = updateTopicState;
    }

    @Override
    public void processEvent(final TypedEvent<TopicEvent> event)
    {
        final TopicEvent topicEvent = event.getValue();

        final DirectBuffer name = topicEvent.getName();
        if (!topicExists.test(name))
        {
            // TODO should never happen ?!
            Loggers.ORCHESTRATION_LOGGER.error("Topic {} was created, but create event was never read.", BufferUtil.bufferAsString(name));
        }
    }

    @Override
    public void updateState(final TypedEvent<TopicEvent> event)
    {
        updateTopicState.accept(new TopicInfo(event.getValue()));
    }
}
