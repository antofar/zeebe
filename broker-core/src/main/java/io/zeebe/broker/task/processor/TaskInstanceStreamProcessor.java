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
package io.zeebe.broker.task.processor;

import static io.zeebe.broker.util.PayloadUtil.isNilPayload;
import static io.zeebe.broker.util.PayloadUtil.isValidPayload;

import org.agrona.DirectBuffer;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.task.CreditsRequest;
import io.zeebe.broker.task.TaskSubscriptionManager;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.map.TaskInstanceMap;
import io.zeebe.broker.transport.clientapi.SubscribedRecordWriter;
import io.zeebe.protocol.clientapi.Intent;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.SubscriptionType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.util.buffer.BufferUtil;

public class TaskInstanceStreamProcessor
{
    protected static final short STATE_CREATED = 1;
    protected static final short STATE_LOCKED = 2;
    protected static final short STATE_FAILED = 3;
    protected static final short STATE_LOCK_EXPIRED = 4;

    protected SubscribedRecordWriter subscribedEventWriter;
    protected final TaskSubscriptionManager taskSubscriptionManager;
    protected final CreditsRequest creditsRequest = new CreditsRequest();

    protected final TaskInstanceMap taskIndex;
    protected int logStreamPartitionId;

    public TaskInstanceStreamProcessor(TaskSubscriptionManager taskSubscriptionManager)
    {
        this.taskSubscriptionManager = taskSubscriptionManager;

        this.taskIndex = new TaskInstanceMap();
    }

    public TypedStreamProcessor createStreamProcessor(TypedStreamEnvironment environment)
    {
        this.logStreamPartitionId = environment.getStream().getPartitionId();
        this.subscribedEventWriter = new SubscribedRecordWriter(environment.getOutput());

        return environment.newStreamProcessor()
            .onEvent(ValueType.TASK, Intent.CREATE, new CreateTaskProcessor())
            .onEvent(ValueType.TASK, Intent.LOCK, new LockTaskProcessor())
            .onEvent(ValueType.TASK, Intent.COMPLETE, new CompleteTaskProcessor())
            .onEvent(ValueType.TASK, Intent.FAIL, new FailTaskProcessor())
            .onEvent(ValueType.TASK, Intent.EXPIRE_LOCK, new ExpireLockTaskProcessor())
            .onEvent(ValueType.TASK, Intent.UPDATE_RETRIES, new UpdateRetriesTaskProcessor())
            .onEvent(ValueType.TASK, Intent.CANCEL, new CancelTaskProcessor())
            .withStateResource(taskIndex.getMap())
            .build();
    }

    private class CreateTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {

        @Override
        public boolean executeSideEffects(TypedRecord<TaskEvent> event, TypedResponseWriter responseWriter)
        {
            boolean success = true;

            if (event.getMetadata().hasRequestMetadata())
            {
                success = responseWriter.writeEvent(Intent.CREATED, event);
            }

            return success;
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            return writer.writeEvent(event.getKey(), Intent.CREATED, event.getValue());
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            taskIndex
                .newTaskInstance(event.getKey())
                .setState(STATE_CREATED)
                .write();
        }
    }

    private class LockTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        protected boolean isLocked;
        protected final CreditsRequest creditsRequest = new CreditsRequest();

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            isLocked = false;

            final short state = taskIndex.wrapTaskInstanceKey(event.getKey()).getState();

            if (state == STATE_CREATED || state == STATE_FAILED || state == STATE_LOCK_EXPIRED)
            {
                isLocked = true;
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<TaskEvent> event, TypedResponseWriter responseWriter)
        {
            boolean success = true;

            if (isLocked)
            {
                final RecordMetadata metadata = event.getMetadata();

                success = subscribedEventWriter
                        .recordType(RecordType.EVENT)
                        .intent(Intent.LOCKED)
                        .partitionId(logStreamPartitionId)
                        .position(event.getPosition())
                        .key(event.getKey())
                        .subscriberKey(metadata.getSubscriberKey())
                        .subscriptionType(SubscriptionType.TASK_SUBSCRIPTION)
                        .valueType(ValueType.TASK)
                        .valueWriter(event.getValue())
                        .tryWriteMessage(metadata.getRequestStreamId());
            }
            else
            {
                final long subscriptionId = event.getMetadata().getSubscriberKey();

                creditsRequest.setSubscriberKey(subscriptionId);
                creditsRequest.setCredits(1);
                success = taskSubscriptionManager.increaseSubscriptionCreditsAsync(creditsRequest);
            }

            return success;
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (isLocked)
            {
                return writer.writeRejection(event);
            }
            else
            {
                return writer.writeEvent(event.getKey(), Intent.LOCKED, event.getValue());
            }
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            if (isLocked)
            {
                taskIndex
                    .setState(STATE_LOCKED)
                    .setLockOwner(event.getValue().getLockOwner())
                    .write();
            }
        }
    }

    // TODO: let's have an abstract class TypedCommandProcessor that receives commands
    // and just decides if the command is accepted or rejected; response and event writing is done in the super class

    private class CompleteTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        protected boolean isCompleted;

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            isCompleted = false;

            taskIndex.wrapTaskInstanceKey(event.getKey());
            final short state = taskIndex.getState();

            final TaskEvent value = event.getValue();

            final boolean isCompletable = state == STATE_LOCKED || state == STATE_LOCK_EXPIRED;
            if (isCompletable)
            {
                final DirectBuffer payload = value.getPayload();
                if (isNilPayload(payload) || isValidPayload(payload))
                {
                    if (BufferUtil.contentsEqual(taskIndex.getLockOwner(), value.getLockOwner()))
                    {
                        isCompleted = true;
                    }
                }
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<TaskEvent> event, TypedResponseWriter responseWriter)
        {
            if (isCompleted)
            {
                return responseWriter.writeEvent(Intent.COMPLETED, event);
            }
            else
            {
                return responseWriter.writeRejection(event);
            }
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (isCompleted)
            {
                return writer.writeEvent(event.getKey(), Intent.COMPLETED, event.getValue());
            }
            else
            {
                return writer.writeRejection(event);
            }
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            if (isCompleted)
            {
                taskIndex.remove(event.getKey());
            }
        }
    }

    private class FailTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        protected boolean isFailed;

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            isFailed = false;

            final TaskEvent value = event.getValue();

            taskIndex.wrapTaskInstanceKey(event.getKey());
            if (taskIndex.getState() == STATE_LOCKED && BufferUtil.contentsEqual(taskIndex.getLockOwner(), value.getLockOwner()))
            {
                isFailed = true;
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<TaskEvent> event, TypedResponseWriter responseWriter)
        {
            if (isFailed)
            {
                return responseWriter.writeEvent(Intent.FAILED, event);
            }
            else
            {
                return responseWriter.writeRejection(event);
            }
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (isFailed)
            {
                return writer.writeEvent(event.getKey(), Intent.FAILED, event.getValue());
            }
            else
            {
                return writer.writeRejection(event);
            }
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            if (isFailed)
            {
                taskIndex
                    .setState(STATE_FAILED)
                    .write();
            }
        }
    }

    private class ExpireLockTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        protected boolean isExpired;

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            isExpired = false;

            taskIndex.wrapTaskInstanceKey(event.getKey());

            if (taskIndex.getState() == STATE_LOCKED)
            {
                isExpired = true;
            }
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (isExpired)
            {
                return writer.writeEvent(event.getKey(), Intent.LOCK_EXPIRED, event.getValue());
            }
            else
            {
                return writer.writeRejection(event);
            }
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            if (isExpired)
            {
                taskIndex
                    .setState(STATE_LOCK_EXPIRED)
                    .write();
            }
        }
    }

    private class UpdateRetriesTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        private boolean success;

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            final short state = taskIndex.wrapTaskInstanceKey(event.getKey()).getState();
            final TaskEvent value = event.getValue();
            success = state == STATE_FAILED && value.getRetries() > 0;
        }

        @Override
        public boolean executeSideEffects(TypedRecord<TaskEvent> event, TypedResponseWriter responseWriter)
        {
            if (success)
            {
                return responseWriter.writeEvent(Intent.RETRIES_UPDATED, event);
            }
            else
            {
                return responseWriter.writeRejection(event);
            }
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (success)
            {
                return writer.writeEvent(event.getKey(), Intent.RETRIES_UPDATED, event.getValue());
            }
            else
            {
                return writer.writeRejection(event);
            }
        }
    }

    private class CancelTaskProcessor implements TypedRecordProcessor<TaskEvent>
    {
        private boolean isCanceled;

        @Override
        public void processRecord(TypedRecord<TaskEvent> event)
        {
            final short state = taskIndex.wrapTaskInstanceKey(event.getKey()).getState();
            isCanceled = state > 0;
        }

        @Override
        public long writeRecord(TypedRecord<TaskEvent> event, TypedStreamWriter writer)
        {
            if (isCanceled)
            {
                return writer.writeEvent(event.getKey(), Intent.CANCELED, event.getValue());
            }
            else
            {
                return writer.writeRejection(event);
            }
        }

        @Override
        public void updateState(TypedRecord<TaskEvent> event)
        {
            if (isCanceled)
            {
                taskIndex.remove(event.getKey());
            }
        }
    }
}
