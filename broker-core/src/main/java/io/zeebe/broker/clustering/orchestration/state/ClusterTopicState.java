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
package io.zeebe.broker.clustering.orchestration.state;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.clustering.orchestration.topic.TopicState;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ServerTransport;
import io.zeebe.util.sched.ActorControl;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ClusterTopicState implements Service<ClusterTopicState>, StreamProcessorLifecycleAware
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final Injector<Partition> partitionInjector = new Injector<>();
    private final Injector<ServerTransport> serverTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();

    private final TopicCreateProcessor topicCreateProcessor;
    private final TopicCreatedProcessor topicCreatedProcessor;

    private final Map<DirectBuffer, TopicInfo> topicState = new HashMap<>();
    private ActorControl actor;

    public ClusterTopicState()
    {
        topicCreateProcessor = new TopicCreateProcessor(this::topicExists, this::updateTopicState);
        topicCreatedProcessor = new TopicCreatedProcessor(this::topicExists, this::completeTopicCreation);
    }

    @Override
    public ClusterTopicState get()
    {
        return this;
    }

    private boolean topicExists(final DirectBuffer directBuffer)
    {
        return topicState.containsKey(directBuffer);
    }

    private void updateTopicState(final TopicInfo topicInfo)
    {
        LOG.debug("Adding topic state: {}", topicInfo);
        topicState.put(topicInfo.getTopicNameBuffer(), topicInfo);
    }

    private void completeTopicCreation(final TopicInfo topicInfo)
    {
        final TopicInfo completedTopic = topicState.get(topicInfo.getTopicNameBuffer());

        if (completedTopic != null)
        {
            LOG.debug("Updating topic state: {}", topicInfo);
            completedTopic.update(topicInfo);
        }
        else
        {
            LOG.warn("Topic not found in state: {}", topicInfo);
            updateTopicState(topicInfo);
        }
    }

    @Override
    public void onOpen(TypedStreamProcessor streamProcessor)
    {
        actor = streamProcessor.getActor();
    }

    @Override
    public void start(final ServiceStartContext startContext)
    {
        final Partition partition = partitionInjector.getValue();
        final ServerTransport serverTransport = serverTransportInjector.getValue();
        final StreamProcessorServiceFactory streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();

        final TypedStreamProcessor streamProcessor = new TypedStreamEnvironment(partition.getLogStream(), serverTransport.getOutput())
            .newStreamProcessor()
            .onEvent(EventType.TOPIC_EVENT, TopicState.CREATE, topicCreateProcessor)
            .onEvent(EventType.TOPIC_EVENT, TopicState.CREATED, topicCreatedProcessor)
            .withListener(this)
            .build();

        streamProcessorServiceFactory.createService(partition, partitionInjector.getInjectedServiceName())
                                     .additionalDependencies(startContext.getServiceName())
                                     .processor(streamProcessor)
                                     .processorId(StreamProcessorIds.CLUSTER_TOPIC_STATE)
                                     .processorName("cluster-topic-state")
                                     .build();
    }

    public Injector<Partition> getPartitionInjector()
    {
        return partitionInjector;
    }

    public Injector<ServerTransport> getServerTransportInjector()
    {
        return serverTransportInjector;
    }

    public Injector<StreamProcessorServiceFactory> getStreamProcessorServiceFactoryInjector()
    {
        return streamProcessorServiceFactoryInjector;
    }

    public ActorFuture<Map<DirectBuffer, TopicInfo>> getDesiredState()
    {
        return actor.call(() -> topicState);
    }

    public ActorFuture<Map<DirectBuffer, TopicInfo>> getPendingTopics()
    {
        return actor.call(this::collectPendingTopics);
    }

    public ActorFuture<Map<DirectBuffer, TopicInfo>> getCreatedTopics()
    {
        return actor.call(this::collectCreatedTopics);
    }

    private Map<DirectBuffer, TopicInfo> collectPendingTopics()
    {
        return topicState.entrySet().stream()
                         .filter(entry -> entry.getValue().getPartitionIds().isEmpty())
                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<DirectBuffer, TopicInfo> collectCreatedTopics()
    {
        return topicState.entrySet().stream()
                  .filter(entry -> !entry.getValue().getPartitionIds().isEmpty())
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
