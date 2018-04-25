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
package io.zeebe.broker.system.deployment.handler;

import java.time.Duration;

import org.agrona.collections.Long2ObjectHashMap;

import io.zeebe.broker.logstreams.processor.StreamProcessorLifecycleAware;
import io.zeebe.broker.logstreams.processor.TypedRecord;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.logstreams.processor.TypedStreamReader;
import io.zeebe.broker.logstreams.processor.TypedStreamWriter;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingDeployments.PendingDeployment;
import io.zeebe.broker.workflow.data.DeploymentEvent;
import io.zeebe.protocol.clientapi.Intent;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.ScheduledTimer;

public class DeploymentTimer implements StreamProcessorLifecycleAware
{
    private final PendingDeployments pendingDeployments;
    private final Duration deploymentTimeout;

    private ActorControl actor;
    private TypedStreamReader reader;
    private TypedStreamWriter writer;

    private Long2ObjectHashMap<ScheduledTimer> timers = new Long2ObjectHashMap<>();

    public DeploymentTimer(PendingDeployments deployments, Duration deploymentTimeout)
    {
        this.pendingDeployments = deployments;
        this.deploymentTimeout = deploymentTimeout;
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        this.actor = streamProcessor.getActor();
        this.reader = streamProcessor.getEnvironment().buildStreamReader();
        this.writer = streamProcessor.getEnvironment().buildStreamWriter();

        for (PendingDeployment deployment : pendingDeployments)
        {
            if (!deployment.isResolved())
            {
                scheduleTimeout(deployment.getDeploymentKey());
            }
        }
    }

    @Override
    public void onClose()
    {
        for (ScheduledTimer timer : timers.values())
        {
            timer.cancel();
        }
        timers.clear();
    }

    private void scheduleTimeout(long deploymentKey)
    {
        final ScheduledTimer timer = actor.runDelayed(deploymentTimeout, () -> timeOutDeployment(deploymentKey));
        timers.put(deploymentKey, timer);
    }

    public void onDeploymentValidated(long key)
    {
        scheduleTimeout(key);
    }

    public void onDeploymentResolved(long key)
    {
        final ScheduledTimer timer = timers.remove(key);
        if (timer != null)
        {
            timer.cancel();
        }
    }

    private void timeOutDeployment(long key)
    {
        final PendingDeployment deployment = pendingDeployments.get(key);

        if (deployment != null)
        {
            final TypedRecord<DeploymentEvent> deploymentEvent = reader.readValue(deployment.getDeploymentEventPosition(), DeploymentEvent.class);

            actor.runUntilDone(() ->
            {
                final long position = writer.writeEvent(deploymentEvent.getKey(), Intent.TIMED_OUT, deploymentEvent.getValue());

                if (position >= 0)
                {
                    actor.done();
                }
                else
                {
                    actor.yield();
                }
            });

        }
    }
}
