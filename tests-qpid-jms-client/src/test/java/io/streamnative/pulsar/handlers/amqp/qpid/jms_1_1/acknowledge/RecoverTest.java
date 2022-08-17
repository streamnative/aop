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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.acknowledge;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RecoverTest.
 */
public class RecoverTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoverTest.class);
    private static final int SENT_COUNT = 4;

    // TODO need message order protection
    @Test
    @Ignore
    public void testRecoverForClientAcknowledge() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, SENT_COUNT);
            connection.start();

            Message message = receiveAndValidateMessage(consumer, 0);
            message.acknowledge();

            receiveAndValidateMessage(consumer, 1);
            receiveAndValidateMessage(consumer, 2);
            session.recover();

            receiveAndValidateMessage(consumer, 1);
            receiveAndValidateMessage(consumer, 2);
            receiveAndValidateMessage(consumer, 3);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testMessageOrderForClientAcknowledge() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, SENT_COUNT);
            connection.start();

            int messageSeen = 0;
            int expectedIndex = 0;
            while (expectedIndex < SENT_COUNT)
            {
                Message message = receiveAndValidateMessage(consumer, expectedIndex);

                //don't ack the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    LOGGER.debug(String.format("Recovering message with index %d", expectedIndex));
                    session.recover();
                    messageSeen++;
                }
                else
                {
                    LOGGER.debug(String.format("Acknowledging message with index %d", expectedIndex));
                    messageSeen = 0;
                    expectedIndex++;
                    message.acknowledge();
                }
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testAcknowledgePerConsumer() throws Exception
    {
        Queue queue1 = createQueue("Q1");
        Queue queue2 = createQueue("Q2");

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer1 = consumerSession.createConsumer(queue1);
            MessageConsumer consumer2 = consumerSession.createConsumer(queue2);

            Connection producerConnection = getConnection();
            try
            {
                Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer producer1 = producerSession.createProducer(queue1);
                MessageProducer producer2 = producerSession.createProducer(queue2);

                producer1.send(producerSession.createTextMessage("msg1"));
                producer2.send(producerSession.createTextMessage("msg2"));
            }
            finally
            {
                producerConnection.close();
            }
            consumerConnection.start();

            TextMessage message2 = (TextMessage) consumer2.receive(getReceiveTimeout());
            assertNotNull(message2);
            assertEquals("msg2", message2.getText());

            message2.acknowledge();
            consumerSession.recover();

            TextMessage message1 = (TextMessage) consumer1.receive(getReceiveTimeout());
            assertNotNull(message1);
            assertEquals("msg1", message1.getText());
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testRecoverInAutoAckListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            Utils.sendMessages(connection, queue, 1);

            final CountDownLatch awaitMessages = new CountDownLatch(2);
            final AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();
            final AtomicInteger deliveryCounter = new AtomicInteger();
            consumer.setMessageListener(message -> {
                try
                {
                    deliveryCounter.incrementAndGet();
                    assertEquals("Unexpected JMSRedelivered", deliveryCounter.get() > 1, message.getJMSRedelivered());
                    if (deliveryCounter.get() == 1)
                    {
                        consumerSession.recover();
                    }
                }
                catch (Throwable e)
                {
                    LOGGER.error("Unexpected failure on message receiving", e);
                    listenerCaughtException.set(e);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            connection.start();

            assertTrue("Message is not received in timely manner",
                       awaitMessages.await(getReceiveTimeout() * 2, TimeUnit.MILLISECONDS));
            assertEquals("Message not received the correct number of times.",
                         2, deliveryCounter.get());
            assertNull("No exception should be caught by listener : " + listenerCaughtException.get(),
                       listenerCaughtException.get());
        }
        finally
        {
            connection.close();
        }
    }

    private Message receiveAndValidateMessage(final MessageConsumer consumer,
                                              final int messageIndex)
            throws JMSException
    {
        Message message = consumer.receive(getReceiveTimeout());
        assertNotNull(String.format("Expected message '%d' is not received", messageIndex), message);
        assertEquals("Received message out of order", messageIndex, message.getIntProperty(INDEX));
        return message;
    }

}
