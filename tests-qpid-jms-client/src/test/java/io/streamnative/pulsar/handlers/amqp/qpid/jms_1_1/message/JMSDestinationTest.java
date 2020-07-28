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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.junit.Ignore;
import org.junit.Test;

/**
 * JMSDestinationTest.
 */
public class JMSDestinationTest extends JmsTestBase
{

    @Test
    @Ignore
    public void messageSentToQueueComesBackWithTheSameJMSDestination() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should not be null", receivedMessage);

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull("JMSDestination should not be null", receivedDestination);
            assertTrue("Unexpected destination type", receivedDestination instanceof Queue);
            assertEquals("Unexpected destination name",
                         queue.getQueueName(),
                         ((Queue) receivedDestination).getQueueName());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void messageSentToTopicComesBackWithTheSameJMSDestination() throws Exception
    {
        Topic topic = createTopic(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(topic);

            Utils.sendMessages(session, topic, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should not be null", receivedMessage);

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull("JMSDestination should not be null", receivedDestination);
            assertTrue("Unexpected destination type", receivedDestination instanceof Topic);
            assertEquals("Unexpected destination name",
                         topic.getTopicName(),
                         ((Topic) receivedDestination).getTopicName());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void messageSentToQueueComesBackWithTheSameJMSDestinationWhenReceivedAsynchronously() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            CountDownLatch receiveLatch = new CountDownLatch(1);
            AtomicReference<Message> messageHolder = new AtomicReference<>();
            consumer.setMessageListener(message -> {
                messageHolder.set(message);
                receiveLatch.countDown();
            });
            assertTrue("Timed out waiting for message to be received ",
                       receiveLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNotNull("Message should not be null", messageHolder.get());

            Destination receivedDestination = messageHolder.get().getJMSDestination();

            assertNotNull("JMSDestination should not be null", receivedDestination);
            assertTrue("Unexpected destination type", receivedDestination instanceof Queue);
            assertEquals("Unexpected destination name",
                         queue.getQueueName(),
                         ((Queue) receivedDestination).getQueueName());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testReceiveResend() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            Utils.sendMessages(session, queue, 1);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should not be null", receivedMessage);

            Destination receivedDestination = receivedMessage.getJMSDestination();

            assertNotNull("JMSDestination should not be null", receivedDestination);
            assertTrue("Unexpected destination type", receivedDestination instanceof Queue);
            assertEquals("Unexpected destination name",
                         queue.getQueueName(),
                         ((Queue) receivedDestination).getQueueName());

            MessageProducer producer = session.createProducer(queue);
            producer.send(receivedMessage);

            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should not be null", message);

            Destination destination = message.getJMSDestination();

            assertNotNull("JMSDestination should not be null", destination);
            assertTrue("Unexpected destination type", destination instanceof Queue);
            assertEquals("Unexpected destination name",
                         queue.getQueueName(),
                         ((Queue) destination).getQueueName());
        }
        finally
        {
            connection.close();
        }
    }
}
