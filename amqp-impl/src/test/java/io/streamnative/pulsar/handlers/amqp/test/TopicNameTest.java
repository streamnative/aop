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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

import io.netty.channel.EventLoopGroup;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
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
        ManagedLedgerImpl managedLedger = Mockito.mock(ManagedLedgerImpl.class);

        BrokerService brokerService = Mockito.mock(BrokerService.class);
        Mockito.when(brokerService.executor()).thenReturn(mock(EventLoopGroup.class));
        PersistentTopic exchangeTopic1 = Mockito.mock(PersistentTopic.class);
        Mockito.when(exchangeTopic1.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + exchangeName);
        Mockito.when(exchangeTopic1.getManagedLedger()).thenReturn(managedLedger);
        Mockito.when(exchangeTopic1.getBrokerService()).thenReturn(brokerService);
        Mockito.when(managedLedger.getCursors()).thenReturn(new ManagedCursorContainer());
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic1, false,
                    Executors.newSingleThreadScheduledExecutor());
        } catch (IllegalArgumentException e) {
            fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic exchangeTopic2 = Mockito.mock(PersistentTopic.class);
        Mockito.when(exchangeTopic2.getName()).thenReturn(PersistentExchange.TOPIC_PREFIX + "_" + exchangeName);
        Mockito.when(exchangeTopic2.getManagedLedger()).thenReturn(managedLedger);
        try {
            new PersistentExchange(
                    exchangeName, exchangeType, exchangeTopic2, false,
                    Executors.newSingleThreadScheduledExecutor());
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

    @Test
    public void queueTopicNameValidate() {
        String queueName = "ex-test";
        ManagedLedgerImpl managedLedger = Mockito.mock(ManagedLedgerImpl.class);

        PersistentTopic queueTopic1 = Mockito.mock(PersistentTopic.class);
        Mockito.when(queueTopic1.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + queueName);
        Mockito.when(queueTopic1.getManagedLedger()).thenReturn(managedLedger);
        try {
            new PersistentQueue(
                    queueName, queueTopic1, 0, false, false);
        } catch (IllegalArgumentException e) {
            fail("Failed to new PersistentExchange. errorMsg: " + e.getMessage());
        }

        PersistentTopic queueTopic2 = Mockito.mock(PersistentTopic.class);
        Mockito.when(queueTopic2.getName()).thenReturn(PersistentQueue.TOPIC_PREFIX + "_" + queueName);
        Mockito.when(queueTopic2.getManagedLedger()).thenReturn(managedLedger);
        try {
            new PersistentQueue(
                    queueName, queueTopic2, 0, false, false);
        } catch (IllegalArgumentException e) {
            assertNotNull(e);
            log.info("This is expected behavior.");
        }
    }

}
