/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.test.broker.protocol.brokerapi;

import static io.zeebe.protocol.clientapi.SubscribedEventEncoder.eventHeaderLength;

import java.util.Map;

import org.agrona.MutableDirectBuffer;

import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.MessageHeaderEncoder;
import io.zeebe.protocol.clientapi.SubscribedEventEncoder;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.test.broker.protocol.MsgPackHelper;
import io.zeebe.test.util.collection.MapBuilder;
import io.zeebe.transport.RemoteAddress;
import io.zeebe.transport.ServerTransport;
import io.zeebe.transport.TransportMessage;
import io.zeebe.util.buffer.BufferWriter;

public class SubscribedEventBuilder implements BufferWriter
{
    protected final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    protected final SubscribedEventEncoder bodyEncoder = new SubscribedEventEncoder();
    protected final TransportMessage message = new TransportMessage();

    protected final MsgPackHelper msgPackHelper;
    protected final ServerTransport transport;

    protected int partitionId;
    protected long position;
    protected long key;
    protected long subscriberKey;
    protected SubscriptionType subscriptionType;
    protected EventType eventType;
    protected byte[] event;

    public SubscribedEventBuilder(MsgPackHelper msgPackHelper, ServerTransport transport)
    {
        this.msgPackHelper = msgPackHelper;
        this.transport = transport;
    }

    public SubscribedEventBuilder partitionId(int partitionId)
    {
        this.partitionId = partitionId;
        return this;
    }

    public SubscribedEventBuilder position(long position)
    {
        this.position = position;
        return this;
    }

    public SubscribedEventBuilder key(long key)
    {
        this.key = key;
        return this;
    }

    public SubscribedEventBuilder subscriberKey(long subscriberKey)
    {
        this.subscriberKey = subscriberKey;
        return this;
    }

    public SubscribedEventBuilder subscriptionType(SubscriptionType subscriptionType)
    {
        this.subscriptionType = subscriptionType;
        return this;
    }

    public SubscribedEventBuilder eventType(EventType eventType)
    {
        this.eventType = eventType;
        return this;
    }

    public SubscribedEventBuilder event(Map<String, Object> event)
    {
        this.event = msgPackHelper.encodeAsMsgPack(event);
        return this;
    }

    public MapBuilder<SubscribedEventBuilder> event()
    {
        return new MapBuilder<>(this, this::event);
    }

    public void push(RemoteAddress target)
    {
        message.reset()
            .remoteAddress(target)
            .writer(this);

        final boolean success = transport.getOutput().sendMessage(message);

        if (!success)
        {
            throw new RuntimeException("Could not schedule message on send buffer");
        }
    }

    @Override
    public int getLength()
    {
        return MessageHeaderEncoder.ENCODED_LENGTH +
                SubscribedEventEncoder.BLOCK_LENGTH +
                eventHeaderLength() +
                event.length;
    }

    @Override
    public void write(MutableDirectBuffer buffer, int offset)
    {
        headerEncoder.wrap(buffer, offset)
            .blockLength(bodyEncoder.sbeBlockLength())
            .schemaId(bodyEncoder.sbeSchemaId())
            .templateId(bodyEncoder.sbeTemplateId())
            .version(bodyEncoder.sbeSchemaVersion());

        bodyEncoder.wrap(buffer, offset + headerEncoder.encodedLength())
            .valueType(eventType)
            .key(key)
            .position(position)
            .subscriberKey(subscriberKey)
            .subscriptionType(subscriptionType)
            .partitionId(partitionId)
            .putEvent(event, 0, event.length);
    }


}
