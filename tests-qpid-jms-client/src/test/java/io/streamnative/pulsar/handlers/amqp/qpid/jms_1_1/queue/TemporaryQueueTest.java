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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import org.junit.Test;

/**
 * TemporaryQueueTest.
 */
public class TemporaryQueueTest extends JmsTestBase
{
    @Test
    public void testMessageDeliveryUsingTemporaryQueue() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull("Temporary queue cannot be null", queue);
            final MessageProducer producer = session.createProducer(queue);
            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            producer.send(session.createTextMessage("hello"));
            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("hello", ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumeFromAnotherConnectionProhibited() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Connection connection2 = getConnection();
            try
            {
                final Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final TemporaryQueue queue = session1.createTemporaryQueue();
                assertNotNull("Temporary queue cannot be null", queue);

                try
                {
                    session2.createConsumer(queue);
                    fail("Expected a JMSException when subscribing to a temporary queue created on a different session");
                }
                catch (JMSException je)
                {
                    //pass
                }
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumeFromAnotherConnectionUsingTemporaryQueueName() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Connection connection2 = getConnection();
            try
            {
                final Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final TemporaryQueue queue = session1.createTemporaryQueue();
                assertNotNull("Temporary queue cannot be null", queue);

                try
                {
                    session2.createConsumer(session2.createQueue(queue.getQueueName()));
                    fail("Expected a JMSException when subscribing to a temporary queue created on a different session");
                }
                catch (JMSException je)
                {
                    //pass
                }
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPublishFromAnotherConnectionAllowed() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Connection connection2 = getConnection();
            try
            {
                final Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final TemporaryQueue queue = session1.createTemporaryQueue();
                assertNotNull("Temporary queue cannot be null", queue);

                MessageProducer producer = session2.createProducer(queue);
                producer.send(session2.createMessage());

                connection.start();
                MessageConsumer consumer = session1.createConsumer(queue);
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Message not received", message);
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClosingConsumerDoesNotDeleteQueue() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull("Temporary queue cannot be null", queue);

            MessageProducer producer = session.createProducer(queue);
            String messageText = "Hello World!";
            producer.send(session.createTextMessage(messageText));

            connection.start();
            session.createConsumer(queue).close();

            MessageConsumer consumer = session.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("Received message not a text message", message instanceof TextMessage);
            assertEquals("Incorrect message text", messageText, ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testClosingSessionDoesNotDeleteQueue() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            final Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session1.createTemporaryQueue();
            assertNotNull("Temporary queue cannot be null", queue);

            MessageProducer producer = session1.createProducer(queue);
            String messageText = "Hello World!";
            producer.send(session1.createTextMessage(messageText));

            session1.close();

            connection.start();
            MessageConsumer consumer = session2.createConsumer(queue);
            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("Received message not a text message", message instanceof TextMessage);
            assertEquals("Incorrect message text", messageText, ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExplicitTemporaryQueueDeletion() throws Exception
    {
        int numberOfQueuesBeforeTest = getQueueCount();
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final TemporaryQueue queue = session.createTemporaryQueue();
            assertNotNull("Temporary queue cannot be null", queue);
            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            try
            {
                queue.delete();
                fail("Expected JMSException : should not be able to delete while there are active consumers");
            }
            catch (JMSException je)
            {
                //pass
            }

            int numberOfQueuesAfterQueueDelete = getQueueCount();
            assertEquals("Unexpected number of queue", 1, numberOfQueuesAfterQueueDelete - numberOfQueuesBeforeTest);

            consumer.close();

            // Now deletion should succeed.
            queue.delete();

            try
            {
                session.createConsumer(queue);
                fail("Exception not thrown");
            }
            catch (JMSException je)
            {
                //pass
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void delete() throws Exception
    {
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue queue = session.createTemporaryQueue();
            MessageProducer producer = session.createProducer(queue);
            try
            {
                producer.send(session.createTextMessage("hello"));
            }
            catch (JMSException e)
            {
                fail("Send to temporary queue should succeed");
            }

            try
            {
                queue.delete();
            }
            catch (JMSException e)
            {
                fail("temporary queue should be deletable");
            }

            try
            {
                producer.send(session.createTextMessage("hello"));
                fail("Send to deleted temporary queue should not succeed");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
        finally
        {
            connection.close();
        }
    }
}
