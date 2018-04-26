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
package io.zeebe.broker.workflow;

import static io.zeebe.broker.workflow.data.WorkflowInstanceEvent.*;
import static io.zeebe.test.broker.protocol.clientapi.TestTopicClient.taskEvents;
import static io.zeebe.test.broker.protocol.clientapi.TestTopicClient.workflowInstanceEvents;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.instance.WorkflowDefinition;
import io.zeebe.test.broker.protocol.clientapi.*;
import org.junit.*;
import org.junit.rules.RuleChain;

public class CancelWorkflowInstanceTest
{
    private static final WorkflowDefinition WORKFLOW = Bpmn.createExecutableWorkflow("process")
            .startEvent()
            .serviceTask("task", t -> t.taskType("test").taskRetries(5))
            .endEvent()
            .done();

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();
    public ClientApiRule apiRule = new ClientApiRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule(brokerRule).around(apiRule);

    private TestTopicClient testClient;

    @Before
    public void init()
    {
        testClient = apiRule.topic();
    }

    @Test
    public void shouldCancelWorkflowInstance()
    {
        // given
        testClient.deploy(WORKFLOW);

        final long workflowInstanceKey = testClient.createWorkflowInstance("process");

        testClient.receiveEvents(workflowInstanceEvents("ACTIVITY_ACTIVATED"));

        // when
        final ExecuteCommandResponse response = cancelWorkflowInstance(workflowInstanceKey);

        // then
        assertThat(response.getEvent()).containsEntry("state", "WORKFLOW_INSTANCE_CANCELED");

        final SubscribedRecord workflowInstanceCanceledEvent = testClient.receiveSingleEvent(workflowInstanceEvents("WORKFLOW_INSTANCE_CANCELED"));

        assertThat(workflowInstanceCanceledEvent.key()).isEqualTo(workflowInstanceKey);
        assertThat(workflowInstanceCanceledEvent.value())
            .containsEntry(PROP_WORKFLOW_BPMN_PROCESS_ID, "process")
            .containsEntry(PROP_WORKFLOW_VERSION, 1)
            .containsEntry(PROP_WORKFLOW_INSTANCE_KEY, workflowInstanceKey)
            .containsEntry(PROP_WORKFLOW_ACTIVITY_ID, "");

        final List<SubscribedRecord> workflowEvents = testClient
                .receiveEvents(workflowInstanceEvents())
                .limit(9)
                .collect(Collectors.toList());

        assertThat(workflowEvents).extracting(e -> e.value().get(PROP_STATE)).containsExactly(
                "CREATE_WORKFLOW_INSTANCE",
                "WORKFLOW_INSTANCE_CREATED",
                "START_EVENT_OCCURRED",
                "SEQUENCE_FLOW_TAKEN",
                "ACTIVITY_READY",
                "ACTIVITY_ACTIVATED",
                "CANCEL_WORKFLOW_INSTANCE",
                "ACTIVITY_TERMINATED",
                "WORKFLOW_INSTANCE_CANCELED");
    }

    @Test
    public void shouldCancelActivityInstance()
    {
        // given
        testClient.deploy(WORKFLOW);

        final long workflowInstanceKey = testClient.createWorkflowInstance("process");

        final SubscribedRecord activityActivatedEvent = testClient.receiveSingleEvent(workflowInstanceEvents("ACTIVITY_ACTIVATED"));

        final ExecuteCommandResponse response = cancelWorkflowInstance(workflowInstanceKey);

        // then
        assertThat(response.getEvent()).containsEntry("state", "WORKFLOW_INSTANCE_CANCELED");

        final SubscribedRecord activityTerminatedEvent = testClient.receiveSingleEvent(workflowInstanceEvents("ACTIVITY_TERMINATED"));

        assertThat(activityTerminatedEvent.key()).isEqualTo(activityActivatedEvent.key());
        assertThat(activityTerminatedEvent.value())
            .containsEntry(PROP_WORKFLOW_BPMN_PROCESS_ID, "process")
            .containsEntry(PROP_WORKFLOW_VERSION, 1)
            .containsEntry(PROP_WORKFLOW_INSTANCE_KEY, workflowInstanceKey)
            .containsEntry(PROP_WORKFLOW_ACTIVITY_ID, "task");
    }

    @Test
    public void shouldCancelTaskForActivity()
    {
        // given
        testClient.deploy(WORKFLOW);

        final long workflowInstanceKey = testClient.createWorkflowInstance("process");

        final SubscribedRecord taskCreatedEvent = testClient.receiveSingleEvent(taskEvents("CREATED"));

        final ExecuteCommandResponse response = cancelWorkflowInstance(workflowInstanceKey);

        // then
        assertThat(response.getEvent()).containsEntry("state", "WORKFLOW_INSTANCE_CANCELED");

        final SubscribedRecord taskCanceledEvent = testClient.receiveSingleEvent(taskEvents("CANCELED"));

        assertThat(taskCanceledEvent.key()).isEqualTo(taskCreatedEvent.key());

        @SuppressWarnings("unchecked")
        final Map<String, Object> headers = (Map<String, Object>) taskCanceledEvent.value().get("headers");
        assertThat(headers)
            .containsEntry("workflowInstanceKey", workflowInstanceKey)
            .containsEntry("bpmnProcessId", "process")
            .containsEntry("workflowDefinitionVersion", 1)
            .containsEntry("activityId", "task");
    }

    @Test
    public void shouldRejectCancelNonExistingWorkflowInstance()
    {
        // when
        final ExecuteCommandResponse response = cancelWorkflowInstance(-1L);

        // then
        assertThat(response.getEvent()).containsEntry("state", "CANCEL_WORKFLOW_INSTANCE_REJECTED");

        testClient.receiveSingleEvent(workflowInstanceEvents("CANCEL_WORKFLOW_INSTANCE_REJECTED"));
    }

    @Test
    public void shouldRejectCancelCompletedWorkflowInstance()
    {
        // given
        testClient.deploy(Bpmn.createExecutableWorkflow("process")
                .startEvent()
                .endEvent()
                .done());

        final long workflowInstanceKey = testClient.createWorkflowInstance("process");

        testClient.receiveSingleEvent(workflowInstanceEvents("WORKFLOW_INSTANCE_COMPLETED"));

        // when
        final ExecuteCommandResponse response = cancelWorkflowInstance(workflowInstanceKey);

        // then
        assertThat(response.getEvent()).containsEntry("state", "CANCEL_WORKFLOW_INSTANCE_REJECTED");

        testClient.receiveSingleEvent(workflowInstanceEvents("CANCEL_WORKFLOW_INSTANCE_REJECTED"));
    }

    private ExecuteCommandResponse cancelWorkflowInstance(final long workflowInstanceKey)
    {
        return apiRule.createCmdRequest()
            .valueTypeWorkflow()
            .key(workflowInstanceKey)
            .command()
                .put("state", "CANCEL_WORKFLOW_INSTANCE")
            .done()
            .sendAndAwait();
    }
}
