package io.zeebe.broker.clustering.orchestration;

import java.util.HashMap;
import java.util.Map;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.partitions.Partition;
import io.zeebe.broker.logstreams.processor.StreamProcessorIds;
import io.zeebe.broker.logstreams.processor.StreamProcessorServiceFactory;
import io.zeebe.broker.logstreams.processor.TypedStreamEnvironment;
import io.zeebe.broker.logstreams.processor.TypedStreamProcessor;
import io.zeebe.broker.system.log.TopicState;
import io.zeebe.protocol.clientapi.EventType;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.transport.ServerTransport;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

public class ClusterTopicState implements Service<ClusterTopicState>
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;

    private final Injector<Partition> partitionInjector = new Injector<>();
    private final Injector<ServerTransport> serverTransportInjector = new Injector<>();
    private final Injector<StreamProcessorServiceFactory> streamProcessorServiceFactoryInjector = new Injector<>();

    private final TopicCreateProcessor topicCreateProcessor;
    private final TopicCreatedProcessor topicCreatedProcessor;

    private final Map<TopicName, TopicInfo> topicState = new HashMap<>();

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
        topicState.put(topicInfo.getTopicName(), topicInfo);
    }

    private void completeTopicCreation(final TopicInfo topicInfo)
    {
        final TopicInfo completedTopic = topicState.get(topicInfo.getTopicName());

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
    public void start(final ServiceStartContext startContext)
    {
        final Partition partition = partitionInjector.getValue();
        final ServerTransport serverTransport = serverTransportInjector.getValue();
        final StreamProcessorServiceFactory streamProcessorServiceFactory = streamProcessorServiceFactoryInjector.getValue();

        final TypedStreamProcessor streamProcessor = new TypedStreamEnvironment(partition.getLogStream(), serverTransport.getOutput())
            .newStreamProcessor()
            .onEvent(EventType.TOPIC_EVENT, TopicState.CREATE, topicCreateProcessor)
            .onEvent(EventType.TOPIC_EVENT, TopicState.CREATED, topicCreatedProcessor)
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

}
