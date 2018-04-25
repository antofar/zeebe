package io.zeebe.broker.system.deployment.processor;

import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedRecordProcessor;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.workflow.data.DeploymentEvent;

public class DeploymentRejectedProcessor implements TypedRecordProcessor<DeploymentEvent>
{
    private final PendingDeployments pendingDeployments;

    public DeploymentRejectedProcessor(PendingDeployments pendingDeployments)
    {
        this.pendingDeployments = pendingDeployments;
    }

    @Override
    public void updateState(TypedRecord<DeploymentEvent> record)
    {
        pendingDeployments.remove(record.getKey());
    }
}
