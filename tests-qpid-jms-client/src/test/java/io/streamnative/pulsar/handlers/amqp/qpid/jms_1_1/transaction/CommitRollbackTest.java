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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.transaction;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.IllegalStateException;
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
 * CommitRollbackTest.
 */
public class CommitRollbackTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitRollbackTest.class);

    @Test
    @Ignore
    public void produceMessageAndAbortTransactionByClosingConnection() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "B", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    @Ignore
    public void produceMessageAndAbortTransactionByClosingSession() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer transactedProducer = transactedSession.createProducer(queue);
            transactedProducer.send(transactedSession.createTextMessage("A"));
            transactedSession.close();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            connection.start();
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            assertEquals("Unexpected message received", "B", ((TextMessage) message).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void produceMessageAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("B"));

            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "B", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void produceMessageAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(queue);
            messageProducer.send(session.createTextMessage("A"));
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndAbortTransactionByClosingConnection() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndAbortTransactionByClosingSession() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer transactedConsumer = transactedSession.createConsumer(queue);
            Message message = transactedConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());

            transactedSession.close();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);

            Message message2 = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message2 instanceof TextMessage);
            assertEquals("Unexpected message received", "A", ((TextMessage) message2).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void receiveMessageAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue, "A");

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertTrue("Text message should be received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected message received", "A", textMessage.getText());
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 1, message.getIntProperty(INDEX));
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageCloseConsumerAndCommitTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
            messageConsumer.close();
            session.commit();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 1, message.getIntProperty(INDEX));
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void receiveMessageCloseConsumerAndRollbackTransaction() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 2);

            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
            messageConsumer.close();
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        Connection connection2 = getConnection();
        try
        {
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection2.start();

            Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull("Message not received", message);
            assertEquals("Unexpected message received", 0, message.getIntProperty(INDEX));
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    @Ignore
    public void transactionSharedByConsumers() throws Exception
    {
        final Queue queue1 = createQueue("Q1");
        final Queue queue2 = createQueue("Q2");
        Connection connection = getConnection();
        try
        {
            Utils.sendTextMessage(connection, queue1, "queue1Message1");
            Utils.sendTextMessage(connection, queue1, "queue1Message2");
            Utils.sendTextMessage(connection, queue2, "queue2Message1");
            Utils.sendTextMessage(connection, queue2, "queue2Message2");

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer1 = session.createConsumer(queue1);
            MessageConsumer messageConsumer2 = session.createConsumer(queue2);
            connection.start();

            Message message1 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue("Text message not received from first queue", message1 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message1",
                         ((TextMessage) message1).getText());

            Message message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message2 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message1",
                         ((TextMessage) message2).getText());

            session.rollback();

            message1 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue("Text message not received from first queue", message1 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message1",
                         ((TextMessage) message1).getText());

            message2 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message2 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message1",
                         ((TextMessage) message2).getText());

            session.commit();

            Message message3 = messageConsumer1.receive(getReceiveTimeout());
            assertTrue("Text message not received from first queue", message3 instanceof TextMessage);
            assertEquals("Unexpected message received from first queue",
                         "queue1Message2",
                         ((TextMessage) message3).getText());

            Message message4 = messageConsumer2.receive(getReceiveTimeout());
            assertTrue("Text message not received from second queue", message4 instanceof TextMessage);
            assertEquals("Unexpected message received from second queue",
                         "queue2Message2",
                         ((TextMessage) message4).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void commitWithinMessageListener() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        int messageNumber = 2;
        try
        {
            Utils.sendMessages(connection, queue, messageNumber);
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final CountDownLatch receiveLatch = new CountDownLatch(messageNumber);
            final AtomicInteger commitCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageConsumerThrowable = new AtomicReference<>();
            MessageConsumer messageConsumer = session.createConsumer(queue);

            messageConsumer.setMessageListener(message -> {
                try
                {
                    LOGGER.info("received message " + message);
                    assertEquals("Unexpected message received", commitCounter.get(), message.getIntProperty(INDEX));
                    LOGGER.info("commit session");
                    session.commit();
                    commitCounter.incrementAndGet();
                }
                catch (Throwable e)
                {
                    messageConsumerThrowable.set(e);
                    LOGGER.error("Unexpected exception", e);
                }
                finally
                {
                    receiveLatch.countDown();
                }
            });
            connection.start();

            assertTrue("Messages not received in expected time",
                       receiveLatch.await(getReceiveTimeout() * messageNumber, TimeUnit.MILLISECONDS));
            assertNull("Unexpected exception: " + messageConsumerThrowable.get(), messageConsumerThrowable.get());
            assertEquals("Unexpected number of commits", messageNumber, commitCounter.get());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void rollbackWithinMessageListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(session, queue, 2);
            connection.start();
            final CountDownLatch receiveLatch = new CountDownLatch(2);
            final AtomicInteger receiveCounter = new AtomicInteger();
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            consumer.setMessageListener(message -> {
                try
                {
                    if (receiveCounter.incrementAndGet()<3)
                    {
                        session.rollback();
                    }
                    else
                    {
                        session.commit();
                        receiveLatch.countDown();
                    }
                }
                catch (Throwable e)
                {
                    messageListenerThrowable.set(e);
                }
            });

            assertTrue("Timeout waiting for messages",
                       receiveLatch.await(getReceiveTimeout() * 4, TimeUnit.MILLISECONDS));
            assertNull("Exception occurred: " + messageListenerThrowable.get(),
                       messageListenerThrowable.get());
            assertEquals("Unexpected number of received messages", 4, receiveCounter.get());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void exhaustedPrefetchInTransaction() throws Exception
    {
        final int maxPrefetch = 2;
        final int messageNumber = maxPrefetch + 1;

        final Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(maxPrefetch).build();
        try
        {
            Utils.sendMessages(connection, queue, messageNumber);
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection.start();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final Message message = messageConsumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message %d not received", i), message);
                assertEquals("Unexpected message received", i, message.getIntProperty(INDEX));
            }

            session.rollback();

            for (int i = 0; i < maxPrefetch; i++)
            {
                final Message message = messageConsumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message %d not received after rollback", i), message);
                assertEquals("Unexpected message received after rollback", i, message.getIntProperty(INDEX));
            }

            session.commit();

            final Message message = messageConsumer.receive(getReceiveTimeout());
            assertNotNull(String.format("Message %d not received", maxPrefetch), message);
            assertEquals("Unexpected message received", maxPrefetch, message.getIntProperty(INDEX));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void testMessageOrder() throws Exception
    {
        final int messageNumber = 4;
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, messageNumber);
            connection.start();

            int messageSeen = 0;
            int expectedIndex = 0;
            while (expectedIndex < messageNumber)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Expected message '%d' is not received", expectedIndex), message);
                assertEquals("Received message out of order", expectedIndex, message.getIntProperty(INDEX));

                //don't commit transaction for the message until we receive it 5 times
                if (messageSeen < 5)
                {
                    // receive remaining
                    for (int m = expectedIndex + 1; m < messageNumber; m++)
                    {
                        Message remaining = consumer.receive(getReceiveTimeout());
                        assertNotNull(String.format("Expected remaining message '%d' is not received", m), message);
                        assertEquals("Received remaining message out of order", m, remaining.getIntProperty(INDEX));
                    }

                    LOGGER.debug(String.format("Rolling back transaction for message with index %d", expectedIndex));
                    session.rollback();
                    messageSeen++;
                }
                else
                {
                    LOGGER.debug(String.format("Committing transaction for message with index %d", expectedIndex));
                    messageSeen = 0;
                    expectedIndex++;
                    session.commit();
                }
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCommitOnClosedConnection() throws Exception
    {
        Session transactedSession;
        Connection connection = getConnection();
        try
        {
            transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        }
        finally
        {
            connection.close();
        }

        assertNotNull("Session cannot be null", transactedSession);
        try
        {
            transactedSession.commit();
            fail("Commit on closed connection should throw IllegalStateException!");
        }
        catch (IllegalStateException e)
        {
            // passed
        }
    }

    @Test
    public void testCommitOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.commit();
                fail("Commit on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testRollbackOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.rollback();
                fail("Rollback on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testGetTransactedOnClosedSession() throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session transactedSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            transactedSession.close();
            try
            {
                transactedSession.getTransacted();
                fail("According to Sun TCK invocation of Session#getTransacted on closed session should throw IllegalStateException!");
            }
            catch (IllegalStateException e)
            {
                // passed
            }
        }
        finally
        {
            connection.close();
        }
    }
}
