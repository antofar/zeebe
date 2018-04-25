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
package io.zeebe.broker.system.deployment.processor;

import org.agrona.collections.LongArrayList;
import org.slf4j.Logger;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.TypedBatchWriter;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingDeployments.PendingDeployment;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflow;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflowIterator;
import io.zeebe.broker.system.deployment.handler.DeploymentTimer;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.broker.workflow.data.WorkflowEvent;
import io.zeebe.protocol.clientapi.Intent;

public class DeploymentTimedOutProcessor implements TypedRecordProcessor<DeploymentEvent>
{
    private static final Logger LOG = Loggers.SYSTEM_LOGGER;

    private final PendingDeployments pendingDeployments;
    private final PendingWorkflows pendingWorkflows;
    private final DeploymentTimer timer;
    private TypedStreamReader reader;

    private final LongArrayList workflowKeys = new LongArrayList();

    private boolean deploymentRejected;

    public DeploymentTimedOutProcessor(
            PendingDeployments pendingDeployments,
            PendingWorkflows pendingWorkflows,
            DeploymentTimer timer)
    {
        this.pendingDeployments = pendingDeployments;
        this.pendingWorkflows = pendingWorkflows;
        this.timer = timer;
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        this.reader = streamProcessor.getEnvironment().buildStreamReader();
    }

    @Override
    public void processRecord(TypedRecord<DeploymentEvent> event)
    {
        final PendingDeployment pendingDeployment = pendingDeployments.get(event.getKey());

        deploymentRejected = pendingDeployment != null && !pendingDeployment.isResolved(); // could be distributed in the meantime

        if (deploymentRejected)
        {
            workflowKeys.clear();
            collectWorkflowKeysForDeployment(event.getKey());

            LOG.info("Creation of deployment with key '{}' timed out. Delete containg workflows with keys: {}", event.getKey(), workflowKeys);
        }

        if (!event.getMetadata().hasRequestMetadata())
        {
            throw new RuntimeException("missing request metadata of deployment");
        }
    }

    private void collectWorkflowKeysForDeployment(final long deploymentKey)
    {
        final PendingWorkflowIterator workflows = pendingWorkflows.iterator();
        while (workflows.hasNext())
        {
            final PendingWorkflow workflow = workflows.next();

            if (workflow.getDeploymentKey() == deploymentKey)
            {
                final long workflowKey = workflow.getWorkflowKey();

                if (!workflowKeys.containsLong(workflowKey))
                {
                    workflowKeys.addLong(workflowKey);
                }
            }
        }
    }

    @Override
    public long writeRecord(TypedRecord<DeploymentEvent> event, TypedStreamWriter writer)
    {
        if (deploymentRejected)
        {
            final TypedBatchWriter batch = writer.newBatch();

            workflowKeys.forEachOrderedLong(workflowKey ->
            {
                final WorkflowEvent workflowEvent = reader.readValue(workflowKey, WorkflowEvent.class).getValue();
                batch.addEvent(workflowKey, Intent.DELETE, workflowEvent);
            });

            // the processor of this event sends the response
            final TypedRecord<DeploymentEvent> deployCommand = getDeployCommand(event.getKey());

            batch.addRejection(deployCommand);

            return batch.write();
        }
        else
        {
            return 0L;
        }
    }

    private TypedRecord<DeploymentEvent> getDeployCommand(long deploymentKey)
    {
        final long deploymentCommandPosition = deploymentKey;
        return reader.readValue(deploymentCommandPosition, DeploymentEvent.class);

    }

    @Override
    public boolean executeSideEffects(TypedRecord<DeploymentEvent> record, TypedResponseWriter responseWriter)
    {
        final TypedRecord<DeploymentEvent> deployCommand = getDeployCommand(record.getKey());
        return responseWriter.writeRejection(deployCommand);
    }

    @Override
    public void updateState(TypedRecord<DeploymentEvent> event)
    {
        if (deploymentRejected)
        {
            final long deploymentKey = event.getKey();

            // mark resolved to avoid a second rejection
            // -- remove the pending deployment when all delete workflow messages are sent while process the reject event
            pendingDeployments.markResolved(deploymentKey);
            timer.onDeploymentResolved(deploymentKey);
        }
    }

    @Override
    public void onClose()
    {
        reader.close();
    }

}
