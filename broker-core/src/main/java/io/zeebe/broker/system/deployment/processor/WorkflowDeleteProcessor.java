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

import static io.zeebe.util.EnsureUtil.ensureGreaterThan;

import org.agrona.collections.IntArrayList;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.logstreams.processor.TypedResponseWriter;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingDeployments.PendingDeployment;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflow;
import io.zeebe.broker.system.deployment.data.PendingWorkflows.PendingWorkflowIterator;
import io.zeebe.broker.system.deployment.data.WorkflowVersions;
import io.zeebe.broker.system.deployment.handler.RemoteWorkflowsManager;
import io.zeebe.broker.workflow.data.WorkflowEvent;
import io.zeebe.protocol.clientapi.Intent;

public class WorkflowDeleteProcessor implements TypedRecordProcessor<WorkflowEvent>
{
    private final PendingDeployments pendingDeployments;
    private final PendingWorkflows pendingWorkflows;
    private final WorkflowVersions workflowVersions;

    private final RemoteWorkflowsManager workflowMessageSender;

    private final IntArrayList partitionIds = new IntArrayList();

    public WorkflowDeleteProcessor(
            PendingDeployments pendingDeployments,
            PendingWorkflows pendingWorkflows,
            WorkflowVersions workflowVersions,
            RemoteWorkflowsManager workflowMessageSender)
    {
        this.pendingDeployments = pendingDeployments;
        this.pendingWorkflows = pendingWorkflows;
        this.workflowVersions = workflowVersions;
        this.workflowMessageSender = workflowMessageSender;
    }

    @Override
    public void processRecord(TypedRecord<WorkflowEvent> event)
    {
        final long workflowKey = event.getKey();

        partitionIds.clear();

        final PendingWorkflowIterator workflows = pendingWorkflows.iterator();
        while (workflows.hasNext())
        {
            final PendingWorkflow workflow = workflows.next();

            if (workflow.getWorkflowKey() == workflowKey)
            {
                partitionIds.addInt(workflow.getPartitionId());
            }
        }

        ensureGreaterThan("partition ids", partitionIds.size(), 0);
    }

    @Override
    public boolean executeSideEffects(TypedRecord<WorkflowEvent> event, TypedResponseWriter responseWriter)
    {
        return workflowMessageSender.deleteWorkflow(
                   partitionIds,
                   event.getKey(),
                   event.getValue());
    }

    @Override
    public long writeRecord(TypedRecord<WorkflowEvent> event, TypedStreamWriter writer)
    {
        return writer.writeEvent(event.getKey(), Intent.DELETED, event.getValue());
    }

    @Override
    public void updateState(TypedRecord<WorkflowEvent> event)
    {
        final long workflowKey = event.getKey();
        final WorkflowEvent workflowEvent = event.getValue();

        for (int partitionId : partitionIds)
        {
            pendingWorkflows.remove(workflowKey, partitionId);
        }

        final PendingDeployment pendingDeployment = pendingDeployments.get(workflowEvent.getDeploymentKey());
        // reset the workflow's version which is incremented on creation
        workflowVersions.setLatestVersion(pendingDeployment.getTopicName(), workflowEvent.getBpmnProcessId(), workflowEvent.getVersion() - 1);
    }

}
