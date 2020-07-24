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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.messagelistener;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import org.junit.Test;

/**
 * MessageListenerTest.
 */
public class MessageListenerTest extends JmsTestBase
{
    private static final int MSG_COUNT = 10;

    @Test
    public void messageListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();
            final MessageConsumer consumer = session.createConsumer(queue);

            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT);
            consumer.setMessageListener(countingMessageListener);

            countingMessageListener.awaitMessages(getReceiveTimeout());

            assertEquals("Unexpected number of outstanding messages", 0, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void synchronousReceiveFollowedByMessageListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();
            final MessageConsumer consumer = session.createConsumer(queue);
            assertNotNull("Could not receive first message synchronously", consumer.receive(getReceiveTimeout()));

            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT - 1);
            consumer.setMessageListener(countingMessageListener);

            countingMessageListener.awaitMessages(getReceiveTimeout());

            assertEquals("Unexpected number of outstanding messages", 0, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void connectionStopThenStart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();

            final MessageConsumer consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            countingMessageListener.awaitMessages(getReceiveTimeout());

            connection.stop();
            assertTrue("Too few messages received after Connection#stop()", countingMessageListener.getReceivedCount() >= messageToReceivedBeforeConnectionStop);

            countingMessageListener.resetLatch();
            connection.start();

            countingMessageListener.awaitMessages(getReceiveTimeout());
            assertEquals("Unexpected number of outstanding messages", 0, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void connectionStopAndMessageListenerChange() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();

            final MessageConsumer consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            CountingMessageListener countingMessageListener1 = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener1);

            countingMessageListener1.awaitMessages(getReceiveTimeout());

            connection.stop();
            assertTrue("Too few messages received after Connection#stop()", countingMessageListener1.getReceivedCount() >= messageToReceivedBeforeConnectionStop);

            CountingMessageListener countingMessageListener2 = new CountingMessageListener(countingMessageListener1.getOutstandingCount());

            consumer.setMessageListener(countingMessageListener2);
            connection.start();

            countingMessageListener2.awaitMessages(getReceiveTimeout());
            assertEquals("Unexpected number of outstanding messages", 0, countingMessageListener2.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void connectionStopHaltsDeliveryToListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();

            final MessageConsumer consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            countingMessageListener.awaitMessages(getReceiveTimeout());
            connection.stop();

            final int outstandingCountAtStop = countingMessageListener.getOutstandingCount();
            countingMessageListener.resetLatch();
            countingMessageListener.awaitMessages(getReceiveTimeout());

            assertEquals("Unexpected number of outstanding messages", outstandingCountAtStop, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void consumerCloseHaltsDeliveryToListener() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();

            final MessageConsumer consumer = session.createConsumer(queue);
            final int messageToReceivedBeforeConnectionStop = MSG_COUNT / 2;
            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT, messageToReceivedBeforeConnectionStop);
            consumer.setMessageListener(countingMessageListener);

            countingMessageListener.awaitMessages(getReceiveTimeout());

            consumer.close();

            final int outstandingCountAtStop = countingMessageListener.getOutstandingCount();
            countingMessageListener.resetLatch();
            countingMessageListener.awaitMessages(getReceiveTimeout());

            assertEquals("Unexpected number of outstanding messages", outstandingCountAtStop, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void twoMessageListeners() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection1 = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Session session2 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session1, queue, MSG_COUNT);

            final MessageConsumer consumer1 = session1.createConsumer(queue);
            final MessageConsumer consumer2 = session2.createConsumer(queue);

            CountingMessageListener countingMessageListener = new CountingMessageListener(MSG_COUNT);
            consumer1.setMessageListener(countingMessageListener);
            consumer2.setMessageListener(countingMessageListener);

            connection1.start();

            countingMessageListener.awaitMessages(getReceiveTimeout());
            assertEquals("Unexpected number of outstanding messages", 0, countingMessageListener.getOutstandingCount());
        }
        finally
        {
            connection1.close();
        }
    }

    @Test
    public void messageListenerDisallowsSynchronousReceive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Utils.sendMessages(session, queue, MSG_COUNT);

            connection.start();

            final MessageConsumer consumer = session.createConsumer(queue);
            consumer.setMessageListener(message -> { });

            try
            {
                consumer.receive();
                fail("Exception not thrown");
            }
            catch (JMSException e)
            {
                // PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

    private final class CountingMessageListener implements MessageListener
    {
        private final AtomicInteger _receivedCount;
        private final AtomicInteger _outstandingMessageCount;
        private volatile CountDownLatch _awaitMessages;

        CountingMessageListener(final int totalExpectedMessageCount)
        {
            this(totalExpectedMessageCount, totalExpectedMessageCount);
        }

        CountingMessageListener(int totalExpectedMessageCount, int numberOfMessagesToAwait)
        {
            _receivedCount = new AtomicInteger(0);
            _outstandingMessageCount = new AtomicInteger(totalExpectedMessageCount);
            _awaitMessages = new CountDownLatch(numberOfMessagesToAwait);
        }

        int getOutstandingCount()
        {
            return _outstandingMessageCount.get();
        }

        int getReceivedCount()
        {
            return _receivedCount.get();
        }

        void resetLatch()
        {
            _awaitMessages = new CountDownLatch(_outstandingMessageCount.get());
        }

        @Override
        public void onMessage(Message message)
        {
            _receivedCount.incrementAndGet();
            _outstandingMessageCount.decrementAndGet();
            _awaitMessages.countDown();
        }

        void awaitMessages(long timeout) throws Exception
        {
            _awaitMessages.await(timeout, TimeUnit.MILLISECONDS);
        }
    }
}
