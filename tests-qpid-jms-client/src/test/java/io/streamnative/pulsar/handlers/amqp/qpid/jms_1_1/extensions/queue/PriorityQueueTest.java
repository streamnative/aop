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
/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.queue.PriorityQueue;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PriorityQueueTest.
 */
@Ignore
public class PriorityQueueTest extends JmsTestBase
{
    private static final int MSG_COUNT = 50;

    @Test
    public void testPriority() throws Exception
    {
        final int priorities = 10;
        final Queue queue = createPriorityQueue(getTestName(), priorities);
        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);
            for (int msg = 0; msg < MSG_COUNT; msg++)
            {
                producer.setPriority(msg % priorities);
                producer.send(nextMessage(producerSession, msg));
            }
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();
            Message previous = null;
            for (int messageCount = 0, expectedPriority = priorities - 1; messageCount < MSG_COUNT; messageCount++)
            {
                Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message '%d' is not received", messageCount), received);
                assertEquals(String.format("Unexpected message '%d' priority", messageCount),
                             expectedPriority,
                             received.getJMSPriority());
                if (previous != null)
                {
                    assertTrue(String.format(
                            "Messages '%d' arrived in unexpected order : previous message '%d' priority is '%d', received message '%d' priority is '%d'",
                            messageCount,
                            previous.getIntProperty("msg"),
                            previous.getJMSPriority(),
                            received.getIntProperty("msg"),
                            received.getJMSPriority()),
                               previous.getJMSPriority() > received.getJMSPriority()
                               || (previous.getJMSPriority() == received.getJMSPriority()
                                   && previous.getIntProperty("msg") < received.getIntProperty("msg")));

                }
                previous = received;
                if (messageCount > 0 && (messageCount + 1) % (MSG_COUNT / priorities) == 0)
                {
                    expectedPriority--;
                }
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }
    
    @Test
    public void testOddOrdering() throws Exception
    {
        final Queue queue = createPriorityQueue(getTestName(), 3);
        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            // In order ABC
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 1));
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 2));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 3));

            // Out of order BAC
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 4));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 5));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 6));

            // Out of order BCA
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 7));
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 8));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 9));

            // Reverse order CBA
            producer.setPriority(1);
            producer.send(nextMessage(producerSession, 10));
            producer.setPriority(4);
            producer.send(nextMessage(producerSession, 11));
            producer.setPriority(9);
            producer.send(nextMessage(producerSession, 12));
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message msg = consumer.receive(getReceiveTimeout());
            assertEquals(1, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(5, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(9, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(12, msg.getIntProperty("msg"));

            msg = consumer.receive(getReceiveTimeout());
            assertEquals(2, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(4, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(7, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(11, msg.getIntProperty("msg"));

            msg = consumer.receive(getReceiveTimeout());
            assertEquals(3, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(6, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(8, msg.getIntProperty("msg"));
            msg = consumer.receive(getReceiveTimeout());
            assertEquals(10, msg.getIntProperty("msg"));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    /**
     * Test that after sending an initial  message with priority 0, it is able to be repeatedly reflected back to the queue using
     * default priority and then consumed again, with separate transacted sessions with prefetch 1 for producer and consumer.
     *
     * Highlighted defect with PriorityQueues resolved in QPID-3927.
     */
    @Test
    public void testMessageReflectionWithPriorityIncreaseOnTransactedSessionsWithPrefetch1() throws Exception
    {
        Queue queue = createPriorityQueue(getTestName(), 10);
        Connection connection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            connection.start();
            final Session producerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);

            //create the consumer, producer, add message listener
            CountDownLatch latch = new CountDownLatch(5);
            MessageConsumer consumer = producerSession.createConsumer(queue);
            MessageProducer producer = producerSession.createProducer(queue);

            ReflectingMessageListener listener =
                    new ReflectingMessageListener(producerSession, producer, consumerSession, latch);
            consumer.setMessageListener(listener);

            //Send low priority 0 message to kick start the asynchronous reflection process
            producer.setPriority(0);
            producer.send(nextMessage(producerSession, 1));
            producerSession.commit();

            //wait for the reflection process to complete
            assertTrue("Test process failed to complete in allowed time", latch.await(10, TimeUnit.SECONDS));
            assertNull("Unexpected throwable encountered", listener.getThrown());
        }
        finally
        {
            connection.close();
        }
    }

    private Queue createPriorityQueue(final String queueName, final int priorities) throws Exception
    {
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.PriorityQueue", Collections.singletonMap(PriorityQueue.PRIORITIES, priorities));
        return createQueue(queueName);
    }

    private Message nextMessage(Session producerSession, int msg) throws JMSException
    {
        Message message = producerSession.createTextMessage("Message: " + msg);
        message.setIntProperty("msg", msg);
        return message;
    }

    private static class ReflectingMessageListener implements MessageListener
    {
        private static final Logger LOGGER = LoggerFactory.getLogger(PriorityQueueTest.ReflectingMessageListener.class);

        private Session _producerSession;
        private Session _consumerSession;
        private CountDownLatch _latch;
        private MessageProducer _producer;
        private long _origCount;
        private Throwable _lastThrown;

        ReflectingMessageListener(final Session producerSession, final MessageProducer producer,
                                  final Session consumerSession, final CountDownLatch latch)
        {
            _latch = latch;
            _origCount = _latch.getCount();
            _producerSession = producerSession;
            _consumerSession = consumerSession;
            _producer = producer;
        }

        @Override
        public void onMessage(final Message message)
        {
            try
            {
                _latch.countDown();
                long msgNum = _origCount - _latch.getCount();
                LOGGER.info("Received message " + msgNum + " with ID: " + message.getIntProperty("msg"));

                if(_latch.getCount() > 0)
                {
                    //reflect the message, updating its ID and using default priority
                    message.clearProperties();
                    message.setIntProperty("msg", (int) msgNum + 1);
                    _producer.setPriority(Message.DEFAULT_PRIORITY);
                    _producer.send(message);
                    _producerSession.commit();
                }

                //commit the consumer session to consume the message
                _consumerSession.commit();
            }
            catch(Throwable t)
            {
                LOGGER.error(t.getMessage(), t);
                _lastThrown = t;
            }
        }

        public Throwable getThrown()
        {
            return _lastThrown;
        }
    }
}
