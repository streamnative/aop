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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;
import org.apache.qpid.server.queue.LastValueQueue;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LastValueQueueTest.
 */
public class LastValueQueueTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LastValueQueueTest.class);

    private static final String MESSAGE_SEQUENCE_NUMBER_PROPERTY = "msg";
    private static final String KEY_PROPERTY = "key";

    private static final int MSG_COUNT = 400;
    private static final int NUMBER_OF_UNIQUE_KEY_VALUES = 10;

    @Test
    public void testConflation() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, false);
        final Connection producerConnection = getConnection();
        try
        {
            Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(queue);

            Message message = producerSession.createMessage();

            message.setStringProperty(KEY_PROPERTY, "A");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 1);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "B");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 2);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "A");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 3);
            producer.send(message);

            message.setStringProperty(KEY_PROPERTY, "B");
            message.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, 4);
            producer.send(message);
        }
        finally
        {
            producerConnection.close();
        }

        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message received1 = consumer.receive(getReceiveTimeout());
            assertNotNull("First message is not received", received1);
            assertEquals("Unexpected key property value", "A", received1.getStringProperty(KEY_PROPERTY));
            assertEquals("Unexpected sequence property value",
                         3,
                         received1.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));

            Message received2 = consumer.receive(getReceiveTimeout());
            assertNotNull("Second message is not received", received2);
            assertEquals("Unexpected key property value", "B", received2.getStringProperty(KEY_PROPERTY));
            assertEquals("Unexpected sequence property value",
                         4,
                         received2.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));

            assertNull("Unexpected message is received", consumer.receive(getReceiveTimeout() / 4));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflationWithRelease() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, false);

        sendMessages(queue, 0, MSG_COUNT / 2);

        Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received", i), received);
                assertEquals("Unexpected message received",
                             MSG_COUNT / 2 - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }

        sendMessages(queue, MSG_COUNT / 2, MSG_COUNT);

        consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received", i), received);
                assertEquals("Unexpected message received",
                             MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflationWithReleaseAfterNewPublish() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, false);

        sendMessages(queue, 0, MSG_COUNT / 2);

        Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received", i), received);
                assertEquals("Unexpected message received",
                             MSG_COUNT / 2 - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }

            sendMessages(queue, MSG_COUNT / 2, MSG_COUNT);

            consumer.close();
            consumerSession.close();
        }
        finally
        {
            consumerConnection.close();
        }

        consumerConnection = getConnection();
        try
        {
            consumerConnection.start();

            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received", i), received);
                assertEquals("Unexpected message received",
                             MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflatedQueueDepth() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, false);

        sendMessages(queue, 0, MSG_COUNT);

        assertEquals(NUMBER_OF_UNIQUE_KEY_VALUES, getTotalDepthOfQueuesMessages());
    }

    @Test
    public void testConflationBrowser() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, true);

        sendMessages(queue, 0, MSG_COUNT);

        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();
            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received", i), received);
                assertEquals("Unexpected message received",
                             MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }

            sendMessages(queue, MSG_COUNT, MSG_COUNT + 1);

            final Message received = consumer.receive(getReceiveTimeout());
            assertNotNull(String.format("Message with key %d is not received", 0), received);
            assertEquals("Unexpected message received",
                         MSG_COUNT,
                         received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testConflation2Browsers() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, true);

        sendMessages(queue, 0, MSG_COUNT);

        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);
            final MessageConsumer consumer2 = consumerSession.createConsumer(queue);

            consumerConnection.start();

            for (int i = 0; i < NUMBER_OF_UNIQUE_KEY_VALUES; i++)
            {
                final Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received by first consumer", i), received);
                assertEquals("Unexpected message received by first consumer",
                             MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));

                final Message received2 = consumer2.receive(getReceiveTimeout());
                assertNotNull(String.format("Message with key %d is not received by second consumer", i), received2);
                assertEquals("Unexpected message received by second consumer",
                             MSG_COUNT - NUMBER_OF_UNIQUE_KEY_VALUES + i,
                             received2.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY));
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void testParallelProductionAndConsumption() throws Exception
    {
        final String queueName = getTestName();
        final Queue queue = createConflationQueue(queueName, KEY_PROPERTY, false);

        int numberOfUniqueKeyValues = 2;
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        try
        {
            // Start producing threads that send messages
            final BackgroundMessageProducer messageProducer1 = new BackgroundMessageProducer(queue,
                                                                                             numberOfUniqueKeyValues);
            final BackgroundMessageProducer messageProducer2 = new BackgroundMessageProducer(queue,
                                                                                             numberOfUniqueKeyValues);

            final Future<?> future1 = executorService.submit(messageProducer1);
            final Future<?> future2 = executorService.submit(messageProducer2);

            final Map<String, Integer> lastReceivedMessages = receiveMessages(messageProducer1, queue);

            future1.get(getReceiveTimeout() * MSG_COUNT, TimeUnit.MILLISECONDS);
            future2.get(getReceiveTimeout() * MSG_COUNT, TimeUnit.MILLISECONDS);

            final Map<String, Integer> lastSentMessages1 = messageProducer1.getMessageSequenceNumbersByKey();
            assertEquals("Unexpected number of last sent messages sent by producer1",
                         numberOfUniqueKeyValues, lastSentMessages1.size());
            final Map<String, Integer> lastSentMessages2 = messageProducer2.getMessageSequenceNumbersByKey();
            assertEquals(lastSentMessages1, lastSentMessages2);

            assertEquals("The last message sent for each key should match the last message received for that key",
                         lastSentMessages1, lastReceivedMessages);

            assertNull("Unexpected exception from background producer thread", messageProducer1.getException());
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private Map<String, Integer> receiveMessages(BackgroundMessageProducer producer, final Queue queue) throws Exception
    {
        producer.waitUntilQuarterOfMessagesSentToEncourageConflation();

        Map<String, Integer> messageSequenceNumbersByKey = new HashMap<>();
        Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {

            Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            LOGGER.info("Starting to receive");

            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumerConnection.start();

            Message message;
            int numberOfShutdownsReceived = 0;
            int numberOfMessagesReceived = 0;
            while (numberOfShutdownsReceived < 2)
            {
                message = consumer.receive(getReceiveTimeout());
                assertNotNull("null received after "
                              + numberOfMessagesReceived
                              + " messages and "
                              + numberOfShutdownsReceived
                              + " shutdowns", message);

                if (message.propertyExists(BackgroundMessageProducer.SHUTDOWN))
                {
                    numberOfShutdownsReceived++;
                }
                else
                {
                    numberOfMessagesReceived++;
                    putMessageInMap(message, messageSequenceNumbersByKey);
                }
            }

            LOGGER.info("Finished receiving.  Received " + numberOfMessagesReceived + " message(s) in total");
        }
        finally
        {
            consumerConnection.close();
        }
        return messageSequenceNumbersByKey;
    }

    private void putMessageInMap(Message message, Map<String, Integer> messageSequenceNumbersByKey) throws JMSException
    {
        String keyValue = message.getStringProperty(KEY_PROPERTY);
        Integer messageSequenceNumber = message.getIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY);
        messageSequenceNumbersByKey.put(keyValue, messageSequenceNumber);
    }

    private final class BackgroundMessageProducer implements Runnable
    {
        static final String SHUTDOWN = "SHUTDOWN";


        private final Queue _queue;

        private volatile Exception _exception;

        private Map<String, Integer> _messageSequenceNumbersByKey = new HashMap<>();
        private CountDownLatch _quarterOfMessagesSentLatch = new CountDownLatch(MSG_COUNT / 4);
        private int _numberOfUniqueKeyValues;

        BackgroundMessageProducer(Queue queue, final int numberOfUniqueKeyValues)
        {
            _queue = queue;
            _numberOfUniqueKeyValues = numberOfUniqueKeyValues;
        }

        void waitUntilQuarterOfMessagesSentToEncourageConflation() throws InterruptedException
        {
            final long latchTimeout = 60000;
            boolean success = _quarterOfMessagesSentLatch.await(latchTimeout, TimeUnit.MILLISECONDS);
            assertTrue("Failed to be notified that 1/4 of the messages have been sent within " + latchTimeout + " ms.",
                       success);
            LOGGER.info("Quarter of messages sent");
        }

        public Exception getException()
        {
            return _exception;
        }

        Map<String, Integer> getMessageSequenceNumbersByKey()
        {
            return Collections.unmodifiableMap(_messageSequenceNumbersByKey);
        }

        @Override
        public void run()
        {
            try
            {
                LOGGER.info("Starting to send in background thread");
                final Connection producerConnection = getConnection();
                try
                {
                    final Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);

                    final MessageProducer backgroundProducer = producerSession.createProducer(_queue);
                    for (int messageNumber = 0; messageNumber < MSG_COUNT; messageNumber++)
                    {

                        final Message message = nextMessage(messageNumber, producerSession, _numberOfUniqueKeyValues);
                        backgroundProducer.send(message);
                        producerSession.commit();

                        putMessageInMap(message, _messageSequenceNumbersByKey);
                        _quarterOfMessagesSentLatch.countDown();
                    }

                    Message shutdownMessage = producerSession.createMessage();
                    shutdownMessage.setBooleanProperty(SHUTDOWN, true);
                    // make sure the shutdown messages have distinct keys because the Qpid Cpp Broker will
                    // otherwise consider them to have the same key.
                    shutdownMessage.setStringProperty(KEY_PROPERTY, Thread.currentThread().getName());

                    backgroundProducer.send(shutdownMessage);
                    producerSession.commit();
                }
                finally
                {
                    producerConnection.close();
                }

                LOGGER.info("Finished sending in background thread");
            }
            catch (Exception e)
            {
                _exception = e;
                LOGGER.warn("Unexpected exception in publisher", e);
            }
        }

    }

    private Queue createConflationQueue(final String queueName,
                                        final String keyProperty, final boolean enforceBrowseOnly) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put(LastValueQueue.LVQ_KEY, keyProperty);
        if (enforceBrowseOnly)
        {
            arguments.put("ensureNondestructiveConsumers", true);
        }
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.LastValueQueue", arguments);
        return createQueue(queueName);
    }

    private Message nextMessage(int msg, Session producerSession) throws JMSException
    {
        return nextMessage(msg, producerSession, NUMBER_OF_UNIQUE_KEY_VALUES);
    }

    private Message nextMessage(int msg, Session producerSession, int numberOfUniqueKeyValues) throws JMSException
    {
        Message send = producerSession.createTextMessage("Message: " + msg);

        final String keyValue = String.valueOf(msg % numberOfUniqueKeyValues);
        send.setStringProperty(KEY_PROPERTY, keyValue);
        send.setIntProperty(MESSAGE_SEQUENCE_NUMBER_PROPERTY, msg);

        return send;
    }

    private void sendMessages(final Queue queue, final int fromIndex, final int toIndex)
            throws JMSException, NamingException
    {
        Connection producerConnection = getConnection();
        try
        {
            Session producerSession = producerConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = producerSession.createProducer(queue);

            for (int msg = fromIndex; msg < toIndex; msg++)
            {
                producer.send(nextMessage(msg, producerSession));
                producerSession.commit();
            }

            producer.close();
            producerSession.close();
        }
        finally
        {
            producerConnection.close();
        }
    }
}
