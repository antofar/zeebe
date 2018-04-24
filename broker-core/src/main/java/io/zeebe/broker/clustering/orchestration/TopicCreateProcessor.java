package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.log.TopicEvent;
import io.zeebe.broker.system.log.TopicState;
import org.agrona.DirectBuffer;

import java.util.function.Consumer;
import java.util.function.Predicate;

public class TopicCreateProcessor implements TypedEventProcessor<TopicEvent>
{
    private final Predicate<DirectBuffer> topicExists;
    private final Consumer<TopicInfo> updateTopicState;

    public TopicCreateProcessor(final Predicate<DirectBuffer> topicExists, final Consumer<TopicInfo> updateTopicState)
    {
        this.topicExists = topicExists;
        this.updateTopicState = updateTopicState;
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        Loggers.ORCHESTRATION_LOGGER.debug("staring");
    }

    @Override
    public void processEvent(final TypedEvent<TopicEvent> event)
    {
        final TopicEvent topicEvent = event.getValue();

        if (topicExists.test(topicEvent.getName()))
        {
            topicEvent.setState(TopicState.CREATE_REJECTED);
        }
        else
        {
            topicEvent.setState(TopicState.CREATING);
        }
    }

    @Override
    public boolean executeSideEffects(final TypedEvent<TopicEvent> event, final TypedResponseWriter responseWriter)
    {
        return true;
        // TODO: readd
        // return responseWriter.write(event);
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
            final TopicInfo topicInfo = new TopicInfo(topicEvent);
            updateTopicState.accept(topicInfo);
        }
    }
}
