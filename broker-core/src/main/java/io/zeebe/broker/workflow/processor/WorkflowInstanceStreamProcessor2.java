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
package io.zeebe.broker.workflow.processor;

import static io.zeebe.broker.util.PayloadUtil.isNilPayload;
import static io.zeebe.broker.util.PayloadUtil.isValidPayload;
import static io.zeebe.protocol.clientapi.EventType.TASK_EVENT;
import static io.zeebe.protocol.clientapi.EventType.WORKFLOW_INSTANCE_EVENT;

import java.util.*;

import io.zeebe.broker.incident.IncidentEventWriter;
import io.zeebe.broker.incident.data.ErrorType;
import io.zeebe.broker.logstreams.processor.MetadataFilter;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.deployment.handler.CreateWorkflowResponseSender;
import io.zeebe.broker.task.data.TaskEvent;
import io.zeebe.broker.task.data.TaskHeaders;
import io.zeebe.broker.task.data.TaskState;
import io.zeebe.broker.transport.clientapi.CommandResponseWriter;
import io.zeebe.broker.workflow.data.*;
import io.zeebe.broker.workflow.map.*;
import io.zeebe.broker.workflow.map.DeployedWorkflow;
import io.zeebe.broker.workflow.map.WorkflowInstanceIndex.WorkflowInstance;
import io.zeebe.logstreams.log.*;
import io.zeebe.logstreams.log.LogStreamBatchWriter.LogEntryBuilder;
import io.zeebe.logstreams.processor.*;
import io.zeebe.logstreams.snapshot.ComposedSnapshot;
import io.zeebe.logstreams.spi.SnapshotSupport;
import io.zeebe.model.bpmn.BpmnAspect;
import io.zeebe.model.bpmn.instance.*;
import io.zeebe.msgpack.el.*;
import io.zeebe.msgpack.mapping.*;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.protocol.clientapi.Intent;
import io.zeebe.protocol.impl.RecordMetadata;
import io.zeebe.util.metrics.Metric;
import io.zeebe.util.metrics.MetricsManager;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;

public class WorkflowInstanceStreamProcessor2 implements StreamProcessor
{
    private static final UnsafeBuffer EMPTY_TASK_TYPE = new UnsafeBuffer("".getBytes());

    // processors ////////////////////////////////////
    protected final WorkflowCreateEventProcessor workflowCreateEventProcessor = new WorkflowCreateEventProcessor();
    protected final WorkflowDeleteEventProcessor workflowDeleteEventProcessor = new WorkflowDeleteEventProcessor();

    protected final CreateWorkflowInstanceEventProcessor createWorkflowInstanceEventProcessor = new CreateWorkflowInstanceEventProcessor();
    protected final WorkflowInstanceCreatedEventProcessor workflowInstanceCreatedEventProcessor = new WorkflowInstanceCreatedEventProcessor();
    protected final CancelWorkflowInstanceProcessor cancelWorkflowInstanceProcessor = new CancelWorkflowInstanceProcessor();

    protected final EventProcessor updatePayloadProcessor = new UpdatePayloadProcessor();

    protected final EventProcessor sequenceFlowTakenEventProcessor = new ActiveWorkflowInstanceProcessor(new SequenceFlowTakenEventProcessor());
    protected final EventProcessor activityReadyEventProcessor = new ActiveWorkflowInstanceProcessor(new ActivityReadyEventProcessor());
    protected final EventProcessor activityActivatedEventProcessor = new ActiveWorkflowInstanceProcessor(new ActivityActivatedEventProcessor());
    protected final EventProcessor activityCompletingEventProcessor = new ActiveWorkflowInstanceProcessor(new ActivityCompletingEventProcessor());

    protected final EventProcessor taskCompletedEventProcessor = new TaskCompletedEventProcessor();
    protected final EventProcessor taskCreatedEventProcessor = new TaskCreatedProcessor();

    protected final Map<BpmnAspect, EventProcessor> aspectHandlers;

    protected Metric workflowInstanceEventCreate;
    protected Metric workflowInstanceEventCanceled;
    protected Metric workflowInstanceEventCompleted;

    {
        aspectHandlers = new EnumMap<>(BpmnAspect.class);

        aspectHandlers.put(BpmnAspect.TAKE_SEQUENCE_FLOW, new ActiveWorkflowInstanceProcessor(new TakeSequenceFlowAspectHandler()));
        aspectHandlers.put(BpmnAspect.CONSUME_TOKEN, new ActiveWorkflowInstanceProcessor(new ConsumeTokenAspectHandler()));
        aspectHandlers.put(BpmnAspect.EXCLUSIVE_SPLIT, new ActiveWorkflowInstanceProcessor(new ExclusiveSplitAspectHandler()));
    }

    // data //////////////////////////////////////////

    protected final RecordMetadata sourceEventMetadata = new RecordMetadata();
    protected final RecordMetadata targetEventMetadata = new RecordMetadata();

    // internal //////////////////////////////////////

    protected final CommandResponseWriter responseWriter;

    protected final WorkflowInstanceIndex workflowInstanceIndex;
    protected final ActivityInstanceMap activityInstanceMap;
    protected final WorkflowDeploymentCache workflowDeploymentCache;
    protected final PayloadCache payloadCache;

    protected final ComposedSnapshot composedSnapshot;

    protected LogStreamReader logStreamReader;
    protected LogStreamBatchWriter logStreamBatchWriter;
    protected IncidentEventWriter incidentEventWriter;

    protected int logStreamPartitionId;
    protected int streamProcessorId;
    protected long eventKey;
    protected long eventPosition;

    protected final MappingProcessor payloadMappingProcessor;
    protected final JsonConditionInterpreter conditionInterpreter = new JsonConditionInterpreter();

    protected final CreateWorkflowResponseSender workflowResponseSender;

    protected LogStream logStream;

    public WorkflowInstanceStreamProcessor(
            CommandResponseWriter responseWriter,
            CreateWorkflowResponseSender createWorkflowResponseSender,
            int deploymentCacheSize,
            int payloadCacheSize)
    {
        this.responseWriter = responseWriter;
        this.logStreamReader = new BufferedLogStreamReader();

        this.workflowDeploymentCache = new WorkflowDeploymentCache(deploymentCacheSize, logStreamReader);
        this.payloadCache = new PayloadCache(payloadCacheSize, logStreamReader);

        this.workflowInstanceIndex = new WorkflowInstanceIndex();
        this.activityInstanceMap = new ActivityInstanceMap();

        this.payloadMappingProcessor = new MappingProcessor(4096);

        this.workflowResponseSender = createWorkflowResponseSender;

        this.composedSnapshot = new ComposedSnapshot(
            workflowInstanceIndex.getSnapshotSupport(),
            activityInstanceMap.getSnapshotSupport(),
            workflowDeploymentCache.getIdVersionSnapshot(),
            workflowDeploymentCache.getKeyPositionSnapshot(),
            payloadCache.getSnapshotSupport());

    }

    @Override
    public SnapshotSupport getStateResource()
    {
        return composedSnapshot;
    }

    @Override
    public void onOpen(StreamProcessorContext context)
    {
        final LogStream logstream = context.getLogStream();
        this.logStreamPartitionId = logstream.getPartitionId();
        this.streamProcessorId = context.getId();

        this.logStreamReader.wrap(logstream);
        this.logStreamBatchWriter = new LogStreamBatchWriterImpl(logstream);
        this.incidentEventWriter = new IncidentEventWriter(sourceEventMetadata, workflowInstanceEvent);

        this.logStream = logstream;

        final MetricsManager metricsManager = context.getActorScheduler().getMetricsManager();
        final String topicName = logstream.getTopicName().getStringWithoutLengthUtf8(0, logstream.getTopicName().capacity());
        final String partitionId = Integer.toString(logstream.getPartitionId());

        workflowInstanceEventCreate = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "created")
            .create();

        workflowInstanceEventCanceled = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "canceled")
            .create();

        workflowInstanceEventCompleted = metricsManager.newMetric("workflow_instance_events_count")
            .type("counter")
            .label("topic", topicName)
            .label("partition", partitionId)
            .label("type", "completed")
            .create();
    }

    @Override
    public void onClose()
    {
        workflowInstanceIndex.close();
        activityInstanceMap.close();
        workflowDeploymentCache.close();
        payloadCache.close();
        logStreamReader.close();

        workflowInstanceEventCreate.close();
        workflowInstanceEventCanceled.close();
        workflowInstanceEventCompleted.close();
    }

    public static MetadataFilter eventFilter()
    {
        return m -> m.getEventType() == EventType.WORKFLOW_INSTANCE_EVENT
                || m.getEventType() == EventType.TASK_EVENT
                || m.getEventType() == EventType.WORKFLOW_EVENT;
    }

    @Override
    public EventProcessor onEvent(LoggedEvent event)
    {
        reset();

        eventKey = event.getKey();
        eventPosition = event.getPosition();

        sourceEventMetadata.reset();
        event.readMetadata(sourceEventMetadata);

        EventProcessor eventProcessor = null;
        switch (sourceEventMetadata.getEventType())
        {
            case WORKFLOW_INSTANCE_EVENT:
                eventProcessor = onWorkflowInstanceEvent(event);
                break;

            case TASK_EVENT:
                eventProcessor = onTaskEvent(event);
                break;

            case WORKFLOW_EVENT:
                eventProcessor = onWorkflowEvent(event);
                break;

            default:
                break;
        }

        return eventProcessor;
    }

    protected void reset()
    {
        activityInstanceMap.reset();
    }

    protected EventProcessor onWorkflowInstanceEvent(LoggedEvent event)
    {
        workflowInstanceEvent.reset();
        event.readValue(workflowInstanceEvent);

        EventProcessor eventProcessor = null;
        switch (workflowInstanceEvent.getState())
        {
            case CREATE_WORKFLOW_INSTANCE:
                eventProcessor = createWorkflowInstanceEventProcessor;
                break;

            case WORKFLOW_INSTANCE_CREATED:
                eventProcessor = workflowInstanceCreatedEventProcessor;
                workflowInstanceEventCreate.incrementOrdered();
                break;

            case CANCEL_WORKFLOW_INSTANCE:
                eventProcessor = cancelWorkflowInstanceProcessor;
                break;

            case WORKFLOW_INSTANCE_CANCELED:
                workflowInstanceEventCanceled.incrementOrdered();
                break;

            case WORKFLOW_INSTANCE_COMPLETED:
                workflowInstanceEventCompleted.incrementOrdered();
                break;

            case SEQUENCE_FLOW_TAKEN:
                eventProcessor = sequenceFlowTakenEventProcessor;
                break;

            case ACTIVITY_READY:
                eventProcessor = activityReadyEventProcessor;
                break;

            case ACTIVITY_ACTIVATED:
                eventProcessor = activityActivatedEventProcessor;
                break;

            case ACTIVITY_COMPLETING:
                eventProcessor = activityCompletingEventProcessor;
                break;

            case START_EVENT_OCCURRED:
            case END_EVENT_OCCURRED:
            case GATEWAY_ACTIVATED:
            case ACTIVITY_COMPLETED:
            {
                final FlowNode currentActivity = getCurrentActivity();
                eventProcessor = aspectHandlers.get(currentActivity.getBpmnAspect());
                break;
            }

            case UPDATE_PAYLOAD:
                eventProcessor = updatePayloadProcessor;
                break;

            default:
                break;
        }

        return eventProcessor;
    }

    protected EventProcessor onTaskEvent(LoggedEvent event)
    {
        taskEvent.reset();
        event.readValue(taskEvent);

        switch (taskEvent.getState())
        {
            case CREATED:
                return taskCreatedEventProcessor;

            case COMPLETED:
                return taskCompletedEventProcessor;

            default:
                return null;
        }
    }

    protected EventProcessor onWorkflowEvent(LoggedEvent event)
    {
        workflowEvent.reset();
        event.readValue(workflowEvent);

        switch (workflowEvent.getState())
        {
            case CREATE:
                return workflowCreateEventProcessor;

            case DELETE:
                return workflowDeleteEventProcessor;

            default:
                return null;
        }
    }

    protected void lookupWorkflowInstanceEvent(long position)
    {
        final boolean found = logStreamReader.seek(position);
        if (found && logStreamReader.hasNext())
        {
            final LoggedEvent event = logStreamReader.next();

            workflowInstanceEvent.reset();
            event.readValue(workflowInstanceEvent);
        }
        else
        {
            throw new IllegalStateException("workflow instance event not found.");
        }
    }

    private <T extends FlowElement> T getActivity(long workflowKey, DirectBuffer activityId)
    {
        final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);

        if (deployedWorkflow != null)
        {
            final Workflow workflow = deployedWorkflow.getWorkflow();
            return workflow.findFlowElementById(activityId);
        }
        else
        {
            throw new RuntimeException("No workflow found for key: " + workflowKey);
        }
    }

    protected long writeWorkflowEvent(LogStreamWriter writer)
    {
        targetEventMetadata.reset();
        targetEventMetadata
                .protocolVersion(Protocol.PROTOCOL_VERSION)
                .valueType(WORKFLOW_INSTANCE_EVENT);

        // don't forget to set the key or use positionAsKey
        return writer
                .metadataWriter(targetEventMetadata)
                .valueWriter(workflowInstanceEvent)
                .tryWrite();
    }

    protected long writeTaskEvent(LogStreamWriter writer)
    {
        targetEventMetadata.reset();
        targetEventMetadata
                .protocolVersion(Protocol.PROTOCOL_VERSION)
                .valueType(TASK_EVENT);

        // don't forget to set the key or use positionAsKey
        return writer
                .metadataWriter(targetEventMetadata)
                .valueWriter(taskEvent)
                .tryWrite();
    }

    protected boolean sendWorkflowInstanceResponse()
    {
        return responseWriter
                .partitionId(logStreamPartitionId)
                .position(eventPosition)
                .key(eventKey)
                .eventWriter(workflowInstanceEvent)
                .tryWriteResponse(sourceEventMetadata.getRequestStreamId(), sourceEventMetadata.getRequestId());
    }

    private final class WorkflowCreateEventProcessor implements TypedRecordProcessor<WorkflowEvent>
    {
        private boolean isNewWorkflow;
        private int partitionId;

        @Override
        public void onOpen(TypedStreamProcessor streamProcessor)
        {
            partitionId = streamProcessor.getEnvironment().getStream().getPartitionId();
        }

        @Override
        public void processRecord(TypedRecord<WorkflowEvent> record)
        {
            isNewWorkflow = !workflowDeploymentCache.hasWorkflow(eventKey);
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowEvent> record, TypedResponseWriter responseWriter)
        {
            return workflowResponseSender.sendCreateWorkflowResponse(
                    partitionId,
                    record.getKey(),
                    record.getValue().getDeploymentKey(),
                    record.getMetadata().getRequestId(),
                    record.getMetadata().getRequestStreamId());
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowEvent> record, TypedStreamWriter writer)
        {
            if (isNewWorkflow)
            {
                return writer.writeEvent(record.getKey(), Intent.CREATED, record.getValue());
            }
            else
            {
                return writer.writeRejection(record);
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowEvent> record)
        {
            if (isNewWorkflow)
            {
                workflowDeploymentCache.addDeployedWorkflow(record.getPosition(), record.getKey(), record.getValue());
            }
        }
    }

    private final class WorkflowDeleteEventProcessor implements TypedRecordProcessor<WorkflowEvent>
    {

        private final WorkflowInstanceEvent workflowInstanceRecord = new WorkflowInstanceEvent();
        private boolean isDeleted;
        private final LongArrayList workflowInstanceKeys = new LongArrayList();

        @Override
        public void processRecord(TypedRecord<WorkflowEvent> record)
        {
            isDeleted = workflowDeploymentCache.getWorkflow(record.getKey()) != null;

            if (isDeleted)
            {
                collectInstancesOfWorkflow();
            }
        }

        private void collectInstancesOfWorkflow()
        {
            workflowInstanceKeys.clear();

            final Iterator<WorkflowInstance> workflowInstances = workflowInstanceIndex.iterator();
            while (workflowInstances.hasNext())
            {
                final WorkflowInstance workflowInstance = workflowInstances.next();

                if (eventKey == workflowInstance.getWorkflowKey())
                {
                    workflowInstanceKeys.addLong(workflowInstance.getKey());
                }
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowEvent> record, TypedStreamWriter writer)
        {
            if (isDeleted)
            {
                final TypedBatchWriter batchWriter = writer.newBatch();

                batchWriter.addEvent(record.getKey(), Intent.DELETED, record.getValue());

                if (!workflowInstanceKeys.isEmpty())
                {
                    final WorkflowEvent workflowEvent = record.getValue();

                    workflowInstanceRecord
                        .setWorkflowKey(record.getKey())
                        .setBpmnProcessId(workflowEvent.getBpmnProcessId())
                        .setVersion(workflowEvent.getVersion());

                    for (int i = 0; i < workflowInstanceKeys.size(); i++)
                    {
                        final long workflowInstanceKey = workflowInstanceKeys.getLong(i);
                        workflowInstanceRecord.setWorkflowInstanceKey(workflowInstanceKey);

                        batchWriter.addEvent(workflowInstanceKey, Intent.CANCEL, workflowInstanceRecord);
                    }
                }

                return batchWriter.write();
            }
            else
            {
                return 0L;
            }
        }

        @Override
        public void updateState(TypedRecord<WorkflowEvent> record)
        {
            if (isDeleted)
            {
                final WorkflowEvent value = record.getValue();
                final DirectBuffer bpmnProcessId = value.getBpmnProcessId();
                final int version = value.getVersion();

                workflowDeploymentCache.removeDeployedWorkflow(record.getKey(), bpmnProcessId, version);
            }
        }
    }

    private final class CreateWorkflowInstanceEventProcessor implements TypedRecordProcessor<WorkflowInstanceEvent>
    {
        private boolean success;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceEvent> record)
        {
            success = false;

            final WorkflowInstanceEvent workflowInstanceEvent = record.getValue();

            long workflowKey = workflowInstanceEvent.getWorkflowKey();
            final DirectBuffer bpmnProcessId = workflowInstanceEvent.getBpmnProcessId();
            final int version = workflowInstanceEvent.getVersion();

            if (workflowKey <= 0)
            {
                if (version > 0)
                {
                    workflowKey = workflowDeploymentCache.getWorkflowKeyByIdAndVersion(bpmnProcessId, version);
                }
                else
                {
                    workflowKey = workflowDeploymentCache.getWorkflowKeyByIdAndLatestVersion(bpmnProcessId);
                }
            }

            if (workflowKey > 0)
            {
                final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);
                final DirectBuffer payload = workflowInstanceEvent.getPayload();

                if (deployedWorkflow != null && (isNilPayload(payload) || isValidPayload(payload)))
                {
                    workflowInstanceEvent
                        .setWorkflowKey(workflowKey)
                        .setBpmnProcessId(deployedWorkflow.getWorkflow().getBpmnProcessId())
                        .setVersion(deployedWorkflow.getVersion())
                        .setWorkflowInstanceKey(record.getKey());

                    success = true;
                }
            }
        }

        @Override
        public boolean executeSideEffects(TypedRecord<WorkflowInstanceEvent> record, TypedResponseWriter responseWriter)
        {
            if (success)
            {
                return responseWriter.writeEvent(Intent.CREATED, record);
            }
            else
            {
                return responseWriter.writeRejection(record);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceEvent> record, TypedStreamWriter writer)
        {
            if (success)
            {
                return writer.writeEvent(record.getKey(), Intent.CREATED, record.getValue());
            }
            else
            {
                return writer.writeRejection(record);
            }
        }
    }

    private final class WorkflowInstanceCreatedEventProcessor implements TypedRecordProcessor<WorkflowInstanceEvent>
    {

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceEvent> record)
        {
            final WorkflowInstanceEvent workflowInstanceEvent = record.getValue();

            final long workflowKey = workflowInstanceEvent.getWorkflowKey();
            final DeployedWorkflow deployedWorkflow = workflowDeploymentCache.getWorkflow(workflowKey);

            if (deployedWorkflow != null)
            {
                final Workflow workflow = deployedWorkflow.getWorkflow();
                final StartEvent startEvent = workflow.getInitialStartEvent();
                final DirectBuffer activityId = startEvent.getIdAsBuffer();

                workflowInstanceEvent
                    .setWorkflowInstanceKey(eventKey)
                    .setActivityId(activityId);
            }
            else
            {
                throw new RuntimeException("No workflow found for key: " + workflowKey);
            }
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceEvent> record, TypedStreamWriter writer)
        {
            return writer.writeEvent(Intent.START_EVENT_OCCURRED, record.getValue());
        }

        @Override
        public void updateState(TypedRecord<WorkflowInstanceEvent> record)
        {
            workflowInstanceIndex
                .newWorkflowInstance(record.getKey())
                .setPosition(record.getPosition())
                .setActiveTokenCount(1)
                .setActivityInstanceKey(-1L)
                .setWorkflowKey(record.getValue().getWorkflowKey())
                .write();
        }
    }

    private final class TakeSequenceFlowAspectHandler implements TypedRecordProcessor<WorkflowInstanceEvent>
    {
        @Override
        public void processRecord(TypedRecord<WorkflowInstanceEvent> record)
        {
            final WorkflowInstanceEvent workflowInstanceEvent = record.getValue();

            final FlowNode flowNode = getActivity(
                    workflowInstanceEvent.getWorkflowKey(), workflowInstanceEvent.getActivityId());

            // the activity has exactly one outgoing sequence flow
            final SequenceFlow sequenceFlow = flowNode.getOutgoingSequenceFlows().get(0);

            workflowInstanceEvent.setActivityId(sequenceFlow.getIdAsBuffer());
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceEvent> record, TypedStreamWriter writer)
        {
            return writer.writeEvent(Intent.SEQUENCE_FLOW_TAKEN, record.getValue());
        }
    }

    private final class ExclusiveSplitAspectHandler implements TypedRecordProcessor<WorkflowInstanceEvent>
    {
        private boolean hasIncident;

        @Override
        public void processRecord(TypedRecord<WorkflowInstanceEvent> record)
        {
            hasIncident = false;

            final WorkflowInstanceEvent workflowInstanceEvent = record.getValue();

            final ExclusiveGateway exclusiveGateway = getActivity(workflowInstanceEvent.getWorkflowKey(), workflowInstanceEvent.getActivityId());

            try
            {
                final SequenceFlow sequenceFlow = getSequenceFlowWithFulfilledCondition(exclusiveGateway);

                if (sequenceFlow != null)
                {
                    workflowInstanceEvent
                        .setState(WorkflowInstanceState.SEQUENCE_FLOW_TAKEN)
                        .setActivityId(sequenceFlow.getIdAsBuffer());
                }
                else
                {
                    incidentEventWriter
                        .reset()
                        .errorType(ErrorType.CONDITION_ERROR)
                        .errorMessage("All conditions evaluated to false and no default flow is set.");

                    hasIncident = true;
                }
            }
            catch (JsonConditionException e)
            {
                incidentEventWriter
                    .reset()
                    .errorType(ErrorType.CONDITION_ERROR)
                    .errorMessage(e.getMessage());

                hasIncident = true;
            }
        }

        private SequenceFlow getSequenceFlowWithFulfilledCondition(ExclusiveGateway exclusiveGateway)
        {
            final List<SequenceFlow> sequenceFlows = exclusiveGateway.getOutgoingSequenceFlowsWithConditions();
            for (int s = 0; s < sequenceFlows.size(); s++)
            {
                final SequenceFlow sequenceFlow = sequenceFlows.get(s);

                final CompiledJsonCondition compiledCondition = sequenceFlow.getCondition();
                final boolean isFulFilled = conditionInterpreter.eval(compiledCondition.getCondition(), workflowInstanceEvent.getPayload());

                if (isFulFilled)
                {
                    return sequenceFlow;
                }
            }
            return exclusiveGateway.getDefaultFlow();
        }

        @Override
        public long writeRecord(TypedRecord<WorkflowInstanceEvent> record, TypedStreamWriter writer)
        {
            if (!hasIncident)
            {
                return writer.writeEvent(Intent.SEQUENCE_FLOW_TAKEN, record.getValue());
            }
            else
            {
                return incidentEventWriter
                        .failureEventPosition(eventPosition)
                        .activityInstanceKey(eventKey)
                        .tryWrite(writer);
            }
        }
    }

    private final class ConsumeTokenAspectHandler implements EventProcessor
    {
        private boolean isCompleted;
        private int activeTokenCount;

        @Override
        public void processEvent()
        {
            isCompleted = false;

            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(workflowInstanceEvent.getWorkflowInstanceKey());

            activeTokenCount = workflowInstance != null ? workflowInstance.getTokenCount() : 0;
            if (activeTokenCount == 1)
            {
                workflowInstanceEvent
                    .setState(WorkflowInstanceState.WORKFLOW_INSTANCE_COMPLETED)
                    .setActivityId("");

                isCompleted = true;
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            long position = 0L;

            if (isCompleted)
            {
                position = writeWorkflowEvent(
                        writer.key(workflowInstanceEvent.getWorkflowInstanceKey()));
            }
            return position;
        }

        @Override
        public void updateState()
        {
            if (isCompleted)
            {
                workflowInstanceIndex.remove(workflowInstanceEvent.getWorkflowInstanceKey());
                payloadCache.remove(workflowInstanceEvent.getWorkflowInstanceKey());
            }
        }
    }

    private final class SequenceFlowTakenEventProcessor implements EventProcessor
    {
        @Override
        public void processEvent()
        {
            final SequenceFlow sequenceFlow = getCurrentActivity();
            final FlowNode targetNode = sequenceFlow.getTargetNode();

            workflowInstanceEvent.setActivityId(targetNode.getIdAsBuffer());

            if (targetNode instanceof EndEvent)
            {
                workflowInstanceEvent.setState(WorkflowInstanceState.END_EVENT_OCCURRED);
            }
            else if (targetNode instanceof ServiceTask)
            {
                workflowInstanceEvent.setState(WorkflowInstanceState.ACTIVITY_READY);
            }
            else if (targetNode instanceof ExclusiveGateway)
            {
                workflowInstanceEvent.setState(WorkflowInstanceState.GATEWAY_ACTIVATED);
            }
            else
            {
                throw new RuntimeException(String.format("Flow node of type '%s' is not supported.", targetNode));
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            return writeWorkflowEvent(writer.positionAsKey());
        }
    }

    private final class ActivityReadyEventProcessor implements EventProcessor
    {
        private final DirectBuffer sourcePayload = new UnsafeBuffer(0, 0);

        private boolean hasIncident;

        @Override
        public void processEvent()
        {
            hasIncident = false;

            workflowInstanceEvent.setState(WorkflowInstanceState.ACTIVITY_ACTIVATED);

            final ServiceTask serviceTask = getCurrentActivity();
            setWorkflowInstancePayload(serviceTask.getInputOutputMapping().getInputMappings());
        }

        private void setWorkflowInstancePayload(Mapping[] mappings)
        {
            sourcePayload.wrap(workflowInstanceEvent.getPayload());
            // only if we have no default mapping we have to use the mapping processor
            if (mappings.length > 0)
            {
                try
                {
                    final int resultLen = payloadMappingProcessor.extract(sourcePayload, mappings);
                    final MutableDirectBuffer buffer = payloadMappingProcessor.getResultBuffer();
                    workflowInstanceEvent.setPayload(buffer, 0, resultLen);
                }
                catch (MappingException e)
                {
                    incidentEventWriter
                        .reset()
                        .errorType(ErrorType.IO_MAPPING_ERROR)
                        .errorMessage(e.getMessage());

                    hasIncident = true;
                }
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            if (!hasIncident)
            {
                return writeWorkflowEvent(writer.key(eventKey));
            }
            else
            {
                return incidentEventWriter
                        .failureEventPosition(eventPosition)
                        .activityInstanceKey(eventKey)
                        .tryWrite(writer);
            }
        }

        @Override
        public void updateState()
        {
            workflowInstanceIndex
                .get(workflowInstanceEvent.getWorkflowInstanceKey())
                .setActivityInstanceKey(eventKey)
                .write();

            activityInstanceMap
                .newActivityInstance(eventKey)
                .setActivityId(workflowInstanceEvent.getActivityId())
                .setTaskKey(-1L)
                .write();

            if (!hasIncident && !isNilPayload(sourcePayload))
            {
                payloadCache.addPayload(workflowInstanceEvent.getWorkflowInstanceKey(), eventPosition, sourcePayload);
            }
        }
    }

    private final class ActivityActivatedEventProcessor implements EventProcessor
    {
        @Override
        public void processEvent()
        {
            final ServiceTask serviceTask = getCurrentActivity();
            final TaskDefinition taskDefinition = serviceTask.getTaskDefinition();

            taskEvent.reset();

            taskEvent
                .setState(TaskState.CREATE)
                .setType(taskDefinition.getTypeAsBuffer())
                .setRetries(taskDefinition.getRetries())
                .setPayload(workflowInstanceEvent.getPayload());

            setTaskHeaders(serviceTask);
        }

        private void setTaskHeaders(ServiceTask serviceTask)
        {
            taskEvent.headers()
                .setBpmnProcessId(workflowInstanceEvent.getBpmnProcessId())
                .setWorkflowDefinitionVersion(workflowInstanceEvent.getVersion())
                .setWorkflowKey(workflowInstanceEvent.getWorkflowKey())
                .setWorkflowInstanceKey(workflowInstanceEvent.getWorkflowInstanceKey())
                .setActivityId(serviceTask.getIdAsBuffer())
                .setActivityInstanceKey(eventKey);

            final io.zeebe.model.bpmn.instance.TaskHeaders customHeaders = serviceTask.getTaskHeaders();
            if (!customHeaders.isEmpty())
            {
                taskEvent.setCustomHeaders(customHeaders.asMsgpackEncoded());
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            return writeTaskEvent(writer.positionAsKey());
        }
    }

    private final class TaskCreatedProcessor implements EventProcessor
    {
        private boolean isActive;

        @Override
        public void processEvent()
        {
            isActive = false;

            final TaskHeaders taskHeaders = taskEvent.headers();
            final long activityInstanceKey = taskHeaders.getActivityInstanceKey();
            if (activityInstanceKey > 0)
            {
                final WorkflowInstance workflowInstance = workflowInstanceIndex.get(taskHeaders.getWorkflowInstanceKey());

                isActive = workflowInstance != null && activityInstanceKey == workflowInstance.getActivityInstanceKey();
            }
        }

        @Override
        public void updateState()
        {
            if (isActive)
            {
                activityInstanceMap
                    .wrapActivityInstanceKey(taskEvent.headers().getActivityInstanceKey())
                    .setTaskKey(eventKey)
                    .write();
            }
        }
    }

    private final class TaskCompletedEventProcessor implements EventProcessor
    {
        private boolean isActivityCompleted;
        private long activityInstanceKey;

        @Override
        public void processEvent()
        {
            isActivityCompleted = false;

            final TaskHeaders taskHeaders = taskEvent.headers();
            activityInstanceKey = taskHeaders.getActivityInstanceKey();

            if (taskHeaders.getWorkflowInstanceKey() > 0 && isTaskOpen(activityInstanceKey))
            {
                workflowInstanceEvent
                    .setState(WorkflowInstanceState.ACTIVITY_COMPLETING)
                    .setBpmnProcessId(taskHeaders.getBpmnProcessId())
                    .setVersion(taskHeaders.getWorkflowDefinitionVersion())
                    .setWorkflowKey(taskHeaders.getWorkflowKey())
                    .setWorkflowInstanceKey(taskHeaders.getWorkflowInstanceKey())
                    .setActivityId(taskHeaders.getActivityId())
                    .setPayload(taskEvent.getPayload());

                isActivityCompleted = true;
            }
        }

        private boolean isTaskOpen(long activityInstanceKey)
        {
            // task key = -1 when activity is left
            return activityInstanceMap.wrapActivityInstanceKey(activityInstanceKey).getTaskKey() == eventKey;
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            return isActivityCompleted ? writeWorkflowEvent(writer.key(activityInstanceKey)) : 0L;
        }

        @Override
        public void updateState()
        {
            if (isActivityCompleted)
            {
                activityInstanceMap
                    .setTaskKey(-1L)
                    .write();
            }
        }
    }

    private final class ActivityCompletingEventProcessor implements EventProcessor
    {
        private boolean hasIncident;

        @Override
        public void processEvent()
        {
            hasIncident = false;

            workflowInstanceEvent.setState(WorkflowInstanceState.ACTIVITY_COMPLETED);

            final ServiceTask serviceTask = getCurrentActivity();
            setWorkflowInstancePayload(serviceTask.getInputOutputMapping().getOutputMappings());
        }

        private void setWorkflowInstancePayload(Mapping[] mappings)
        {
            final DirectBuffer workflowInstancePayload = payloadCache.getPayload(workflowInstanceEvent.getWorkflowInstanceKey());
            final DirectBuffer taskPayload = workflowInstanceEvent.getPayload();
            final boolean isNilPayload = isNilPayload(taskPayload);

            if (mappings.length > 0)
            {
                if (isNilPayload)
                {
                    incidentEventWriter
                        .reset()
                        .errorType(ErrorType.IO_MAPPING_ERROR)
                        .errorMessage("Task was completed without an payload - processing of output mapping failed!");

                    hasIncident = true;
                }
                else
                {
                    mergePayload(mappings, workflowInstancePayload, taskPayload);
                }
            }
            else if (isNilPayload)
            {
                // no payload from task complete
                workflowInstanceEvent.setPayload(workflowInstancePayload, 0, workflowInstancePayload.capacity());
            }
        }

        private void mergePayload(Mapping[] mappings, final DirectBuffer workflowInstancePayload, final DirectBuffer taskPayload)
        {
            try
            {
                final int resultLen = payloadMappingProcessor.merge(taskPayload, workflowInstancePayload, mappings);
                final MutableDirectBuffer buffer = payloadMappingProcessor.getResultBuffer();
                workflowInstanceEvent.setPayload(buffer, 0, resultLen);
            }
            catch (MappingException e)
            {
                incidentEventWriter
                    .reset()
                    .errorType(ErrorType.IO_MAPPING_ERROR)
                    .errorMessage(e.getMessage());

                hasIncident = true;
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            if (!hasIncident)
            {
                return writeWorkflowEvent(writer.key(eventKey));
            }
            else
            {
                return incidentEventWriter
                        .failureEventPosition(eventPosition)
                        .activityInstanceKey(eventKey)
                        .tryWrite(writer);
            }
        }

        @Override
        public void updateState()
        {
            if (!hasIncident)
            {
                workflowInstanceIndex
                    .get(workflowInstanceEvent.getWorkflowInstanceKey())
                    .setActivityInstanceKey(-1L)
                    .write();

                activityInstanceMap.remove(eventKey);
            }
        }
    }

    private final class CancelWorkflowInstanceProcessor implements EventProcessor
    {
        private final WorkflowInstanceEvent activityInstanceEvent = new WorkflowInstanceEvent();

        private boolean isCanceled;
        private long activityInstanceKey;
        private long taskKey;

        @Override
        public void processEvent()
        {
            isCanceled = false;

            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(eventKey);

            if (workflowInstance != null && workflowInstance.getTokenCount() > 0)
            {
                lookupWorkflowInstanceEvent(workflowInstance.getPosition());

                workflowInstanceEvent
                    .setState(WorkflowInstanceState.WORKFLOW_INSTANCE_CANCELED)
                    .setPayload(WorkflowInstanceEvent.NO_PAYLOAD);

                activityInstanceKey = workflowInstance.getActivityInstanceKey();
                taskKey = activityInstanceMap.wrapActivityInstanceKey(activityInstanceKey).getTaskKey();

                isCanceled = true;
            }
            else
            {
                workflowInstanceEvent.setState(WorkflowInstanceState.CANCEL_WORKFLOW_INSTANCE_REJECTED);
            }
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            logStreamBatchWriter
                .producerId(streamProcessorId)
                .sourceEvent(logStreamPartitionId, eventPosition);

            if (taskKey > 0)
            {
                writeCancelTaskEvent(logStreamBatchWriter.event(), taskKey);
            }

            if (activityInstanceKey > 0)
            {
                writeTerminateActivityInstanceEvent(logStreamBatchWriter.event(), activityInstanceKey);
            }

            writeWorklowInstanceEvent(logStreamBatchWriter.event());

            return logStreamBatchWriter.tryWrite();
        }

        private void writeWorklowInstanceEvent(LogEntryBuilder logEntryBuilder)
        {
            targetEventMetadata.reset();
            targetEventMetadata
                    .protocolVersion(Protocol.PROTOCOL_VERSION)
                    .valueType(WORKFLOW_INSTANCE_EVENT);

            logEntryBuilder
                .key(eventKey)
                .metadataWriter(targetEventMetadata)
                .valueWriter(workflowInstanceEvent)
                .done();
        }

        private void writeCancelTaskEvent(LogEntryBuilder logEntryBuilder, long taskKey)
        {
            targetEventMetadata.reset();
            targetEventMetadata
                .protocolVersion(Protocol.PROTOCOL_VERSION)
                .valueType(TASK_EVENT);

            taskEvent.reset();
            taskEvent
                .setState(TaskState.CANCEL)
                .setType(EMPTY_TASK_TYPE)
                .headers()
                    .setBpmnProcessId(workflowInstanceEvent.getBpmnProcessId())
                    .setWorkflowDefinitionVersion(workflowInstanceEvent.getVersion())
                    .setWorkflowInstanceKey(eventKey)
                    .setActivityId(activityInstanceMap.getActivityId())
                    .setActivityInstanceKey(activityInstanceKey);

            logEntryBuilder
                .key(taskKey)
                .metadataWriter(targetEventMetadata)
                .valueWriter(taskEvent)
                .done();
        }

        private void writeTerminateActivityInstanceEvent(LogEntryBuilder logEntryBuilder, long activityInstanceKey)
        {
            targetEventMetadata.reset();
            targetEventMetadata
                    .protocolVersion(Protocol.PROTOCOL_VERSION)
                    .valueType(WORKFLOW_INSTANCE_EVENT);

            activityInstanceEvent.reset();
            activityInstanceEvent
                .setState(WorkflowInstanceState.ACTIVITY_TERMINATED)
                .setBpmnProcessId(workflowInstanceEvent.getBpmnProcessId())
                .setVersion(workflowInstanceEvent.getVersion())
                .setWorkflowInstanceKey(eventKey)
                .setActivityId(activityInstanceMap.getActivityId());

            logEntryBuilder
                .key(activityInstanceKey)
                .metadataWriter(targetEventMetadata)
                .valueWriter(activityInstanceEvent)
                .done();
        }

        @Override
        public boolean executeSideEffects()
        {
            return sendWorkflowInstanceResponse();
        }

        @Override
        public void updateState()
        {
            if (isCanceled)
            {
                workflowInstanceIndex.remove(eventKey);
                payloadCache.remove(eventKey);
                activityInstanceMap.remove(activityInstanceKey);
            }
        }
    }

    private final class UpdatePayloadProcessor implements EventProcessor
    {
        private boolean isUpdated;

        @Override
        public void processEvent()
        {
            isUpdated = false;

            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(workflowInstanceEvent.getWorkflowInstanceKey());
            final boolean isActive = workflowInstance != null && workflowInstance.getTokenCount() > 0;

            WorkflowInstanceState workflowInstanceEventType = WorkflowInstanceState.UPDATE_PAYLOAD_REJECTED;
            if (isActive && isValidPayload(workflowInstanceEvent.getPayload()))
            {
                workflowInstanceEventType = WorkflowInstanceState.PAYLOAD_UPDATED;
                isUpdated = true;
            }
            workflowInstanceEvent.setState(workflowInstanceEventType);
        }

        @Override
        public boolean executeSideEffects()
        {
            return sendWorkflowInstanceResponse();
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            return writeWorkflowEvent(writer.key(eventKey));
        }

        @Override
        public void updateState()
        {
            if (isUpdated)
            {
                payloadCache.addPayload(workflowInstanceEvent.getWorkflowInstanceKey(), eventPosition, workflowInstanceEvent.getPayload());
            }
        }
    }

    private final class ActiveWorkflowInstanceProcessor implements EventProcessor
    {
        private final EventProcessor processor;

        private boolean isActive;

        ActiveWorkflowInstanceProcessor(EventProcessor processor)
        {
            this.processor = processor;
        }

        @Override
        public void processEvent()
        {
            final WorkflowInstance workflowInstance = workflowInstanceIndex.get(workflowInstanceEvent.getWorkflowInstanceKey());
            isActive = workflowInstance != null && workflowInstance.getTokenCount() > 0;

            if (isActive)
            {
                processor.processEvent();
            }
        }

        @Override
        public boolean executeSideEffects()
        {
            return isActive ? processor.executeSideEffects() : true;
        }

        @Override
        public long writeEvent(LogStreamWriter writer)
        {
            return isActive ? processor.writeEvent(writer) : 0L;
        }

        @Override
        public void updateState()
        {
            if (isActive)
            {
                processor.updateState();
            }
        }
    }

}
