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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.consumerpriority;

import static junit.framework.TestCase.assertNull;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * ConsumerPriorityTest.
 */
public class ConsumerPriorityTest extends JmsTestBase
{
    private static final String LEGACY_BINDING_URL = "BURL:direct://amq.direct/%s/%s?x-priority='%d'";
    private static final String LEGACY_ADDRESS_URL = "ADDR:%s; { create: always, node: { type: queue },"
                                                     + "link : { x-subscribe: { arguments : { x-priority : '%d' } } } }";

    @Test
    public void testLowPriorityConsumerReceivesMessages() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull("There should be no messages in the queue", consumer.receive(getShortReceiveTimeout()));

                Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = producingSession.createProducer(queue);
                producer.send(producingSession.createTextMessage(getTestName()));
                assertNotNull("Expected message is not received", consumer.receive(getReceiveTimeout()));
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailable() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        final String consumerQueue = String.format(LEGACY_BINDING_URL, queueName, queueName, 10);
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queueName, consumerQueue);
    }

    @Test
    public void testLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityConsumerAvailableUsingADDR()
            throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        String consumerQueue = String.format(LEGACY_ADDRESS_URL, queueName, 10);
        doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(queueName, consumerQueue);
    }

    private void doTestLowPriorityConsumerDoesNotReceiveMessagesIfHigherPriorityAvailable(final String queueName,
                                                                                          final String consumerQueue)
            throws Exception
    {
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                final MessageConsumer consumer =
                        consumingSession.createConsumer(consumingSession.createQueue(consumerQueue));
                assertNull("There should be no messages in the queue", consumer.receive(getShortReceiveTimeout()));

                final Connection secondConsumingConnection = getConnection();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                    secondConsumingConnection.start();

                    final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(queue);
                    assertNull("There should be no messages in the queue",
                               standardPriorityConsumer.receive(getShortReceiveTimeout()));

                    final Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageProducer producer = producingSession.createProducer(queue);
                    producer.send(producingSession.createTextMessage(getTestName()));
                    assertNull("Message should not go to the low priority consumer",
                               consumer.receive(getShortReceiveTimeout()));
                    producer.send(producingSession.createTextMessage(getTestName() + " 2"));
                    assertNull("Message should not go to the low priority consumer",
                               consumer.receive(getShortReceiveTimeout()));

                    assertNotNull(standardPriorityConsumer.receive(getReceiveTimeout()));
                    assertNotNull(standardPriorityConsumer.receive(getReceiveTimeout()));
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerHasNoCredit() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        final Connection consumingConnection = getConnection();
        try
        {
            final Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                final Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();
                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull("There should be no messages in the queue", consumer.receive(getShortReceiveTimeout()));

                final Connection secondConsumingConnection = getConnectionBuilder().setPrefetch(2).build();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    secondConsumingConnection.start();
                    final MessageConsumer standardPriorityConsumer = secondConsumingSession.createConsumer(queue);
                    assertNull("There should be no messages in the queue",
                               standardPriorityConsumer.receive(getShortReceiveTimeout()));

                    final Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    final MessageProducer producer = producingSession.createProducer(queue);

                    producer.send(createTextMessage(1, producingSession));
                    assertNull("Message should not go to the low priority consumer",
                               consumer.receive(getShortReceiveTimeout()));
                    producer.send(createTextMessage(2, producingSession));
                    assertNull("Message should not go to the low priority consumer",
                               consumer.receive(getShortReceiveTimeout()));
                    producer.send(createTextMessage(3, producingSession));
                    final Message message = consumer.receive(getReceiveTimeout());
                    assertNotNull(
                            "Message should go to the low priority consumer as standard priority consumer has no credit",
                            message);
                    assertTrue("Message is not a text message", message instanceof TextMessage);
                    assertEquals(getTestName() + " 3", ((TextMessage) message).getText());
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    @Test
    public void testLowPriorityConsumerReceiveMessagesIfHigherPriorityConsumerDoesNotSelect() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        final Queue queue = createQueue(queueName);
        Connection consumingConnection = getConnection();
        try
        {
            Connection producingConnection = getConnectionBuilder().setSyncPublish(true).build();
            try
            {
                Session consumingSession = consumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                consumingConnection.start();

                Session producingSession = producingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

                final Queue consumerDestination =
                        consumingSession.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 10));
                final MessageConsumer consumer = consumingSession.createConsumer(consumerDestination);
                assertNull("There should be no messages in the queue", consumer.receive(getShortReceiveTimeout()));

                final Connection secondConsumingConnection = getConnection();
                try
                {
                    final Session secondConsumingSession =
                            secondConsumingConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    secondConsumingConnection.start();
                    final MessageConsumer standardPriorityConsumer =
                            secondConsumingSession.createConsumer(queue,
                                                                  "msg <> 2");
                    assertNull("There should be no messages in the queue", standardPriorityConsumer.receive(
                            getShortReceiveTimeout()));

                    final MessageProducer producer = producingSession.createProducer(queue);

                    producer.send(createTextMessage(1, producingSession));
                    assertNull("Message should not go to the low priority consumer", consumer.receive(
                            getShortReceiveTimeout()));
                    producer.send(createTextMessage(2, producingSession));
                    Message message = consumer.receive(getReceiveTimeout());
                    assertNotNull(
                            "Message should go to the low priority consumer as standard priority consumer is not interested",
                            message);
                    assertTrue("Message is not a text message", message instanceof TextMessage);
                    assertEquals(getTestName() + " 2", ((TextMessage) message).getText());
                }
                finally
                {
                    secondConsumingConnection.close();
                }
            }
            finally
            {
                producingConnection.close();
            }
        }
        finally
        {
            consumingConnection.close();
        }
    }

    private long getShortReceiveTimeout()
    {
        return getReceiveTimeout() / 4;
    }


    private TextMessage createTextMessage(final int msgId, final Session producingSession) throws JMSException
    {
        TextMessage textMessage = producingSession.createTextMessage(getTestName() + " " + msgId);
        textMessage.setIntProperty("msg", msgId);
        return textMessage;
    }
}
