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
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.zeebe.broker.it.ClientRule;
import io.zeebe.broker.it.EmbeddedBrokerRule;
import io.zeebe.client.TopicsClient;
import io.zeebe.client.event.Event;
import io.zeebe.client.event.TaskEvent;
import io.zeebe.client.topic.Partition;
import io.zeebe.client.topic.Topic;
import io.zeebe.client.topic.Topics;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class CreateTopicTest
{

    public EmbeddedBrokerRule brokerRule = new EmbeddedBrokerRule();

    public ClientRule clientRule = new ClientRule(true);

    @Rule
    public RuleChain ruleChain = RuleChain
        .outerRule(brokerRule)
        .around(clientRule);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Rule
    public Timeout testTimeout = Timeout.seconds(20);

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
        final TopicsClient client = clientRule.topics();

        // when
        final Future<Event> foo = client.create("foo", 2).executeAsync();
        final Future<Event> bar = client.create("bar", 2).executeAsync();

        // then
        assertThat(bar.get(10, TimeUnit.SECONDS).getState()).isEqualTo("CREATING");
        assertThat(foo.get(10, TimeUnit.SECONDS).getState()).isEqualTo("CREATING");

        // when
        clientRule.waitUntilTopicsExists("foo", "bar");

        // then
        final Topics topics = client.getTopics().execute();

        final Optional<Topic> fooTopic = topics.getTopics().stream().filter(t -> t.getName().equals("foo")).findFirst();
        assertThat(fooTopic).hasValueSatisfying(t -> assertThat(t.getPartitions()).hasSize(2));

        final Optional<Topic> barTopic = topics.getTopics().stream().filter(t -> t.getName().equals("bar")).findFirst();
        assertThat(barTopic).hasValueSatisfying(t -> assertThat(t.getPartitions()).hasSize(2));

    }

    @Test
    public void shouldRequestTopics() throws InterruptedException
    {
        // given
        clientRule.topics().create("foo", 2).execute();

        clientRule.waitUntilTopicsExists("foo");

        // when
        final Map<String, List<Partition>> topics = clientRule.topicsByName();

        // then
        assertThat(topics).hasSize(2);
        assertThat(topics.get("foo")).hasSize(2);
        assertThat(topics.get(clientRule.getDefaultTopic())).hasSize(1);
    }

}
