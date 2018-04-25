package io.zeebe.broker.clustering.orchestration;

import io.zeebe.broker.Loggers;
import io.zeebe.broker.clustering.base.topology.PartitionInfo;
import io.zeebe.broker.clustering.base.topology.TopologyManager;
import io.zeebe.broker.clustering.orchestration.state.ClusterTopicState;
import io.zeebe.broker.clustering.orchestration.state.TopicInfo;
import io.zeebe.servicecontainer.Injector;
import io.zeebe.servicecontainer.Service;
import io.zeebe.servicecontainer.ServiceStartContext;
import io.zeebe.servicecontainer.ServiceStopContext;
import io.zeebe.util.buffer.BufferUtil;
import io.zeebe.util.sched.Actor;
import io.zeebe.util.sched.future.ActorFuture;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.*;

public class TopicCreationReviserService extends Actor implements Service<Void>
{
    private static final Logger LOG = Loggers.ORCHESTRATION_LOGGER;
    public static final Duration TIMER_RATE = Duration.ofSeconds(1);


    private final Injector<ClusterTopicState> stateInjector = new Injector<>();
    private final Injector<TopologyManager> topologyManagerInjector = new Injector<>();

    private ClusterTopicState clusterTopicState;
    private TopologyManager topologyManager;

    @Override
    public void start(ServiceStartContext startContext)
    {
        clusterTopicState = stateInjector.getValue();
        topologyManager = topologyManagerInjector.getValue();

        startContext.async(startContext.getScheduler().submitActor(this));
    }

    @Override
    public void stop(ServiceStopContext stopContext)
    {
        stopContext.async(actor.close());
    }


    @Override
    protected void onActorStarted()
    {
        actor.runAtFixedRate(TIMER_RATE, this::checkCurrentState);
    }

    private void checkCurrentState()
    {
        final ActorFuture<Map<DirectBuffer, TopicInfo>> stateFuture = clusterTopicState.getDesiredState();

        actor.runOnCompletion(stateFuture, (desiredState, getDesiredStateError) ->
        {
            if (getDesiredStateError == null)
            {
                final ActorFuture<Map<DirectBuffer, List<PartitionInfo>>> queryFuture = topologyManager.query(readableTopology ->
                {
                    final Map<DirectBuffer, List<PartitionInfo>> currentState = new HashMap<>();

                    final Collection<PartitionInfo> partitions = readableTopology.getPartitions();

                    partitions.forEach(partitionInfo ->
                        currentState.compute(partitionInfo.getTopicName(),
                            (s, partitionInfos) ->
                            {
                                if (partitionInfos == null)
                                {
                                    partitionInfos = new ArrayList<>();
                                }
                                partitionInfos.add(partitionInfo);
                                return partitionInfos;
                            }));

                    return currentState;
                });

                actor.runOnCompletion(queryFuture, (currentState, readTopologyError) ->
                {
                    if (readTopologyError == null)
                    {
                        for (Map.Entry<DirectBuffer, TopicInfo> desiredEntry : desiredState.entrySet())
                        {
                            final TopicInfo desiredTopic = desiredEntry.getValue();

                            final List<PartitionInfo> partitionInfos = currentState.get(desiredTopic.getTopicName());
                            if (partitionInfos == null || partitionInfos.size() < desiredTopic.getPartitionCount())
                            {
                                LOG.debug("Send create partition");
                                // TODO no partitions or not enough for this topic so we create partitions requests
                                // we need Id generation for that
                            }
                            else
                            {
                               //     partitionInfos.size() == desiredTopic.getPartitionCount()
                                // TODO write topic CREATED
                                LOG.debug("Enough partitions created. Current state equals to desired state. Writing Topic {} CREATED.",
                                    BufferUtil.bufferAsString(desiredTopic.getTopicName()));
                            }
                        }
                    }
                    else
                    {
                        Loggers.ORCHESTRATION_LOGGER.error("Error in reading topology.", getDesiredStateError);
                    }
                });
            }
            else
            {
                Loggers.ORCHESTRATION_LOGGER.error("Error in getting desired state.", getDesiredStateError);
            }

        });

    }

    @Override
    public Void get()
    {
        return null;
    }

    public Injector<ClusterTopicState> getStateInjector()
    {
        return stateInjector;
    }

    public Injector<TopologyManager> getTopologyManagerInjector()
    {
        return topologyManagerInjector;
    }
}
