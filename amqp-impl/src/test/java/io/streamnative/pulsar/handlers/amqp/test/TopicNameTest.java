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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import io.netty.channel.EventLoopGroup;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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
        ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);

        BrokerService brokerService = mock(BrokerService.class);
        when(brokerService.executor()).thenReturn(mock(EventLoopGroup.class));
        PersistentTopic exchangeTopic1 = mock(PersistentTopic.class);
        when(exchangeTopic1.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + exchangeName);
        when(exchangeTopic1.getManagedLedger()).thenReturn(managedLedger);
        when(exchangeTopic1.getBrokerService()).thenReturn(brokerService);
        when(managedLedger.getCursors()).thenReturn(new ManagedCursorContainer());
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic1, false);
        } catch (IllegalArgumentException e) {
            fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic exchangeTopic2 = mock(PersistentTopic.class);
        when(exchangeTopic2.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + "_" + exchangeName);
        when(exchangeTopic2.getManagedLedger()).thenReturn(managedLedger);
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic2, false);
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

    @Test
    public void queueTopicNameValidate() {
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        when(pulsarService.getExecutor()).thenReturn(executorService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);

        String queueName = "ex-test";
        ManagedLedgerImpl managedLedger = mock(ManagedLedgerImpl.class);

        PersistentTopic queueTopic1 = mock(PersistentTopic.class);
        when(queueTopic1.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + queueName);
        when(queueTopic1.getManagedLedger()).thenReturn(managedLedger);
        when(queueTopic1.getBrokerService()).thenReturn(brokerService);
        try {
            new PersistentQueue(
                    queueName, queueTopic1, 0, false, false, 5000);
        } catch (IllegalArgumentException e) {
            fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic queueTopic2 = mock(PersistentTopic.class);
        when(queueTopic2.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + "_" + queueName);
        when(queueTopic2.getManagedLedger()).thenReturn(managedLedger);
        when(queueTopic2.getBrokerService()).thenReturn(brokerService);
        try {
            new PersistentQueue(
                    queueName, queueTopic2, 0, false, false, 5000);
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

}
