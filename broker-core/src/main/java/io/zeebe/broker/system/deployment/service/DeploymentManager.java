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
package io.zeebe.broker.system.deployment.service;

import java.time.Duration;

import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.logstreams.processor.StreamProcessorIds;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.TypedEventStreamProcessorBuilder;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.system.SystemConfiguration;
import io.zeebe.broker.system.deployment.data.PendingDeployments;
import io.zeebe.broker.system.deployment.data.PendingWorkflows;
import io.zeebe.broker.system.deployment.data.TopicPartitions;
import io.zeebe.broker.system.deployment.data.WorkflowVersions;
import io.zeebe.broker.system.deployment.handler.DeploymentEventWriter;
import io.zeebe.broker.system.deployment.handler.DeploymentTimer;
import io.zeebe.broker.system.deployment.handler.RemoteWorkflowsManager;
import io.zeebe.broker.system.deployment.processor.DeploymentCreateProcessor;
import io.zeebe.broker.system.deployment.processor.DeploymentDistributedProcessor;
import io.zeebe.broker.system.deployment.processor.DeploymentRejectedProcessor;
import io.zeebe.broker.system.deployment.processor.DeploymentTimedOutProcessor;
import io.zeebe.broker.system.deployment.processor.DeploymentValidatedProcessor;
import io.zeebe.broker.system.deployment.processor.PartitionCollector;
import io.zeebe.broker.system.deployment.processor.WorkflowCreateProcessor;
import io.zeebe.broker.system.deployment.processor.WorkflowDeleteProcessor;
import io.zeebe.protocol.clientapi.Intent;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceGroupReference;
import io.zeebe.servicecontainer.ServiceName;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ClientTransport;
import io.zeebe.transport.ServerTransport;

public class DeploymentManager implements Service<DeploymentManager>
{
    private final ServiceGroupReference<Partition> partitionsGroupReference = ServiceGroupReference.<Partition>create()
        .onAdd((name, partition) -> installDeploymentStreamProcessor(partition, name))
        .build();

    private final Injector<ClientTransport> managementClientInjector = new Injector<>();
    private final Injector<ServerTransport> clientApiTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();

    private final SystemConfiguration systemConfiguration;

    private ClientTransport managementClient;
    private ServerTransport clientApiTransport;
    private TopologyManager topologyManager;
    private StreamProcessorServiceFactory streamProcessorServiceFactory;

    public DeploymentManager(SystemConfiguration systemConfiguration)
    {
        this.systemConfiguration = systemConfiguration;
    }

    @Override
    public void start(ServiceStartContext startContext)
    {
        serviceContext = startContext;
        topologyManager = topologyManagerInjector.getValue();
        managementClient = managementClientInjector.getValue();
        clientApiTransport = clientApiTransportInjector.getValue();
        streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();
    }

    private void installDeploymentStreamProcessor(final Partition partition, ServiceName<Partition> partitionServiceName)
    {
        final WorkflowVersions workflowVersions = new WorkflowVersions();
        final PendingDeployments pendingDeployments = new PendingDeployments();
        final PendingWorkflows pendingWorkflows = new PendingWorkflows();

        final Duration deploymentRequestTimeout = Duration.ofSeconds(systemConfiguration.getDeploymentCreationTimeoutSeconds());




        final TypedStreamEnvironment streamEnvironment = new TypedStreamEnvironment(partition.getLogStream(), clientApiTransport.getOutput());

        final RemoteWorkflowsManager remoteManager = new RemoteWorkflowsManager(pendingDeployments,
            pendingWorkflows,
            topologyManager,
            streamEnvironment,
            managementClient);

        final TypedStreamProcessor streamProcessor = createDeploymentStreamProcessor(workflowVersions,
            pendingDeployments,
            pendingWorkflows,
            deploymentRequestTimeout,
            streamEnvironment,
            deploymentEventWriter,
            remoteManager);

        streamProcessorServiceFactory.createService(partition, partitionServiceName)
            .processor(streamProcessor)
            .processorId(StreamProcessorIds.DEPLOYMENT_PROCESSOR_ID)
            .processorName("deployment")
            .build();
    }

    public static TypedStreamProcessor createDeploymentStreamProcessor(
            final WorkflowVersions workflowVersions,
            final PendingDeployments pendingDeployments,
            final PendingWorkflows pendingWorkflows,
            final Duration deploymentTimeout,
            final TypedStreamEnvironment streamEnvironment,
            final DeploymentEventWriter eventWriter,
            final RemoteWorkflowsManager remoteManager)
    {

        final TypedEventStreamProcessorBuilder streamProcessorBuilder = streamEnvironment.newStreamProcessor();

        final PartitionCollector partitionCollector = new PartitionCollector();
        partitionCollector.registerWith(streamProcessorBuilder);
        final TopicPartitions partitions = partitionCollector.getPartitions();

        final DeploymentTimer timer = new DeploymentTimer(pendingDeployments, eventWriter, deploymentTimeout);

        final TypedStreamProcessor streamProcessor = streamProcessorBuilder
            .onCommand(ValueType.DEPLOYMENT, Intent.CREATE, new DeploymentCreateProcessor(partitions, workflowVersions, pendingDeployments))
            .onEvent(ValueType.DEPLOYMENT, Intent.VALIDATED, new DeploymentValidatedProcessor(pendingDeployments, timer))
            .onCommand(ValueType.WORKFLOW, Intent.CREATE, new WorkflowCreateProcessor(partitions, pendingDeployments, pendingWorkflows, remoteManager))
            .onEvent(ValueType.DEPLOYMENT, Intent.DISTRIBUTED, new DeploymentDistributedProcessor(pendingDeployments, pendingWorkflows, timer))
            .onEvent(ValueType.DEPLOYMENT, Intent.TIMED_OUT, new DeploymentTimedOutProcessor(pendingDeployments, pendingWorkflows, timer))
            .onCommand(ValueType.WORKFLOW, Intent.DELETE, new WorkflowDeleteProcessor(pendingDeployments, pendingWorkflows, workflowVersions, remoteManager))
            .onRejection(ValueType.DEPLOYMENT, Intent.CREATE, new DeploymentRejectedProcessor(pendingDeployments))
            .withStateResource(workflowVersions.getRawMap())
            .withStateResource(pendingDeployments.getRawMap())
            .withStateResource(pendingWorkflows.getRawMap())
            .withListener(eventWriter)
            .withListener(timer)
            .withListener(remoteManager)
            .build();
        return streamProcessor;
    }

    @Override
    public DeploymentManager get()
    {
        return this;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }

    public Injector<ClientTransport> getManagementClientInjector()
    {
        return managementClientInjector;
    }
    public ServiceGroupReference<Partition> getPartitionsGroupReference()
    {
        return partitionsGroupReference;
    }

    public Injector<ServerTransport> getClientApiTransportInjector()
    {
        return clientApiTransportInjector;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }


}
