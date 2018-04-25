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
package io.zeebe.broker.system.log;

import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.protocol.Protocol;
import org.agrona.DirectBuffer;

public class CreateTopicProcessor implements TypedEventProcessor<TopicEvent>
{
    protected final TopicsIndex topics;
    protected final PartitionIdGenerator idGenerator;
    protected final PartitionCreatorSelectionStrategy creatorStrategy;

    public CreateTopicProcessor(
            TopicsIndex topics,
            PartitionIdGenerator idGenerator,
            PartitionCreatorSelectionStrategy creatorStrategy)
    {
        this.topics = topics;
        this.idGenerator = idGenerator;
        this.creatorStrategy = creatorStrategy;
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        this.topics.put(Protocol.SYSTEM_TOPIC_BUF, 0, -1); // ensure that the system topic cannot be created
    }

    @Override
    public void processEvent(TypedEvent<TopicEvent> event)
    {
        final TopicEvent value = event.getValue();

        final DirectBuffer nameBuffer = value.getName();
        final boolean topicExists = topics.moveTo(nameBuffer);

        if (topicExists || value.getPartitions() <= 0)
        {
            value.setState(TopicState.CREATE_REJECTED);
        }
    }

    @Override
    public boolean executeSideEffects(TypedEvent<TopicEvent> event, TypedResponseWriter responseWriter)
    {
        if (event.getValue().getState() == TopicState.CREATE_REJECTED)
        {
            return responseWriter.write(event);
        }
        else
        {
            return true;
        }
    }

    @Override
    public long writeEvent(TypedEvent<TopicEvent> event, TypedStreamWriter writer)
    {
        long position = 0;

        final TopicEvent value = event.getValue();
        if (value.getState() == TopicState.CREATE_REJECTED)
        {
            position = writer.writeFollowupEvent(event.getKey(), event.getValue());
        }

        return position;
    }

    @Override
    public void updateState(TypedEvent<TopicEvent> event)
    {
        final TopicEvent value = event.getValue();
        if (value.getState() != TopicState.CREATE_REJECTED)
        {
            topics.put(value.getName(), value.getPartitions(), event.getPosition());
            idGenerator.moveToNextIds(value.getPartitions());
        }
    }
}
