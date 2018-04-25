/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.broker.it.topic;

import static io.zeebe.test.util.TestUtil.waitUntil;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.zeebe.client.clustering.impl.BrokerPartitionState;
import io.zeebe.client.clustering.impl.TopologyResponse;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.client.TopicsClient;
import io.zeebe.client.event.Event;
import io.zeebe.client.event.TaskEvent;
import io.zeebe.client.topic.Partition;
import io.zeebe.client.topic.Topic;
import io.zeebe.client.topic.Topics;

public class CreateTopicTest
{


    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule();

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

//    @Rule
//    public Timeout testTimeout = Timeout.seconds(20);

    @Test
    public void shouldCreateTaskOnNewTopic()
    {
        // given
        clientRule.topics().create("newTopic", 2).execute();

        // when
        final TaskEvent taskEvent = clientRule.tasks().create("newTopic", "foo").execute();

        // then
        assertThat(taskEvent.getState()).isEqualTo("CREATED");
    }

    @Test
    public void shouldCreateMultipleTopicsInParallel() throws Exception
    {
        // given
        final TopicsClient topics = clientRule.topics();

        // when
        final Future<Event> foo = topics.create("foo", 2).executeAsync();
        final Future<Event> bar = topics.create("bar", 2).executeAsync();

        // then
        assertThat(bar.get(10, TimeUnit.SECONDS).getState()).isEqualTo("CREATED");
        assertThat(foo.get(10, TimeUnit.SECONDS).getState()).isEqualTo("CREATED");
    }

    @Test
    public void shouldRequestTopics() throws InterruptedException
    {
        // given
        final TopicsClient topics = clientRule.topics();
        final Event topicEvent = topics.create("foo", 2).execute();

        assertThat(topicEvent.getState()).isEqualTo("CREATED");

        waitUntil(() ->
            clientRule.getClient().topics().getTopics().execute().getTopics().size() >= 2
        );

        // when
        final Topics returnedTopics = clientRule.topics().getTopics().execute();

        // then
        assertThat(returnedTopics.getTopics()).hasSize(2);
        final Map<String, List<Partition>> topicsByName =
                returnedTopics.getTopics()
                    .stream()
                    .collect(Collectors.toMap(Topic::getName, Topic::getPartitions));

        assertThat(topicsByName.get("foo")).hasSize(2);
        assertThat(topicsByName.get(ClientRule.DEFAULT_TOPIC)).hasSize(1);

        Thread.sleep(2000);
    }

}
