/**
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
package io.streamnative.pulsar.handlers.amqp.test;

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.junit.Assert;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Exchange and queue topic name validation test.
 */
@Slf4j
public class TopicNameTest {

    @Test
    public void exchangeTopicNameValidate() {
        String exchangeName = "ex-test";
        AbstractAmqpExchange.Type exchangeType = AbstractAmqpExchange.Type.Direct;

        PersistentTopic exchangeTopic1 = Mockito.mock(PersistentTopic.class);
        Mockito.when(exchangeTopic1.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + exchangeName);
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic1, null, false);
        } catch (IllegalArgumentException e) {
            Assert.fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic exchangeTopic2 = Mockito.mock(PersistentTopic.class);
        Mockito.when(exchangeTopic2.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + "_" + exchangeName);
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic2, null, false);
        } catch (IllegalArgumentException e) {
            Assert.assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

    @Test
    public void queueTopicNameValidate() {
        String queueName = "ex-test";
        AbstractAmqpExchange.Type exchangeType = AbstractAmqpExchange.Type.Direct;

        PersistentTopic queueTopic1 = Mockito.mock(PersistentTopic.class);
        Mockito.when(queueTopic1.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + queueName);
        try {
            new PersistentQueue(
                    queueName, queueTopic1, 0, false, false);
        } catch (IllegalArgumentException e) {
            Assert.fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic queueTopic2 = Mockito.mock(PersistentTopic.class);
        Mockito.when(queueTopic2.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + "_" + queueName);
        try {
            new PersistentQueue(
                    queueName, queueTopic2, 0, false, false);
        } catch (IllegalArgumentException e) {
            Assert.assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

}
