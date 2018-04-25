/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.clustering.orchestration.state;

import java.util.function.Consumer;
import java.util.function.Predicate;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.TypedEvent;
import io.zeebe.broker.logstreams.processor.TypedEventProcessor;
import io.zeebe.broker.clustering.orchestration.topic.TopicEvent;
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
        updateTopicState.accept(new TopicInfo(event.getPosition(), event.getValue()));
    }
}
