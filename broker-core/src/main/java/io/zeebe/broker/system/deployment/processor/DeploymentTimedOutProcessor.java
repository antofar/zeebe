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

import static io.zeebe.broker.workflow.data.DeploymentState.REJECT;

import java.util.function.Consumer;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingDeployments.PendingDeployment;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflow;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflowIterator;
import io.zeebe.broker.system.deployment.handler.DeploymentTimer;
import io.zeebe.broker.workflow.data.*;
import io.zeebe.protocol.impl.RecordMetadata;
import org.agrona.collections.LongArrayList;
import org.slf4j.Logger;

public class DeploymentTimedOutProcessor implements TypedRecordProcessor<DeploymentEvent>
{
    private static final Logger LOG = Loggers.SYSTEM_LOGGER;

    private final PendingDeployments pendingDeployments;
    private final PendingWorkflows pendingWorkflows;
    private final TypedStreamReader reader;
    private final DeploymentTimer timer;

    private final LongArrayList workflowKeys = new LongArrayList();

    public DeploymentTimedOutProcessor(
            PendingDeployments pendingDeployments,
            PendingWorkflows pendingWorkflows,
            DeploymentTimer timer,
            TypedStreamReader reader)
    {
        this.pendingDeployments = pendingDeployments;
        this.pendingWorkflows = pendingWorkflows;
        this.timer = timer;
        this.reader = reader;
    }

    @Override
    public void processEvent(TypedRecord<DeploymentEvent> event)
    {
        final PendingDeployment pendingDeployment = pendingDeployments.get(event.getKey());

        if (pendingDeployment != null && !pendingDeployment.isResolved())
        {
            /*
             * TODO: braucht man REJECT überhaupt im Lifecycle noch? oder kann man Workflow DELETE sofort schreiben,
             *   wenn man TIMED_OUT sieht und das valide ist? Würde ungerne die REJECTION-Idee hier brechen;
             *   oder geht es hier eigentlich um Fehlschlag, sodass FAILED der richtige Name wäre?
             *   oder sollte man dem User TIMED_OUT schon schicken?
             */

            event.getValue().setState(REJECT);

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
        if (event.getValue().getState() == REJECT)
        {
            final TypedBatchWriter batch = writer.newBatch();

            workflowKeys.forEachOrderedLong(workflowKey ->
            {
                final WorkflowEvent workflowEvent = reader.readValue(workflowKey, WorkflowEvent.class).getValue();
                workflowEvent.setState(WorkflowState.DELETE);

                batch.addFollowUpEvent(workflowKey, workflowEvent);
            });

            // the processor of this event sends the response
            batch.addFollowUpEvent(event.getKey(), event.getValue(), copyRequestMetadata(event));

            return batch.write();
        }
        else
        {
            return 0L;
        }
    }

    private Consumer<RecordMetadata> copyRequestMetadata(TypedRecord<DeploymentEvent> event)
    {
        final RecordMetadata metadata = event.getMetadata();
        return m -> m
                .requestId(metadata.getRequestId())
                .requestStreamId(metadata.getRequestStreamId());
    }

    @Override
    public void updateState(TypedRecord<DeploymentEvent> event)
    {
        final DeploymentEvent deploymentEvent = event.getValue();

        if (deploymentEvent.getState() == REJECT)
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
