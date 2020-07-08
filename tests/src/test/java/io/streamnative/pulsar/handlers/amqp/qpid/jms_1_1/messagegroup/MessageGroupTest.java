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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.messagegroup;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import static javax.jms.Session.SESSION_TRANSACTED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.queue.MessageGroupType;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MessageGroupTest.
 */
public class MessageGroupTest extends JmsTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageGroupTest.class);

    @Test
    public void simpleGroupAssignment() throws Exception
    {
        simpleGroupAssignment(false, false);
    }

    @Test
    public void sharedGroupSimpleGroupAssignment() throws Exception
    {
        simpleGroupAssignment(true, false);
    }

    @Test
    public void simpleGroupAssignmentWithJMSXGroupID() throws Exception
    {
        simpleGroupAssignment(false, true);
    }

    @Test
    public void sharedGroupSimpleGroupAssignmentWithJMSXGroupID() throws Exception
    {
        simpleGroupAssignment(true, true);
    }

    /**
     * Pre populate the queue with messages with groups as follows
     *
     *  ONE
     *  TWO
     *  ONE
     *  TWO
     *
     *  Create two consumers with prefetch of 1, the first consumer should then be assigned group ONE, the second
     *  consumer assigned group TWO if they are started in sequence.
     *
     *  Thus doing
     *
     *  c1 <--- (ONE)
     *  c2 <--- (TWO)
     *  c2 ack --->
     *
     *  c2 should now be able to receive a second message from group TWO (skipping over the message from group ONE)
     *
     *  i.e.
     *
     *  c2 <--- (TWO)
     *  c2 ack --->
     *  c1 <--- (ONE)
     *  c1 ack --->
     *
     */
    private void simpleGroupAssignment(boolean sharedGroups, final boolean useDefaultGroup) throws Exception
    {
        final String groupKey = getGroupKey(useDefaultGroup);
        String queueName = getTestName();
        Destination queue = createQueue(queueName, sharedGroups, useDefaultGroup);

        final Connection producerConnection = getConnection();
        try
        {
            producerConnection.start();
            final Session producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            String[] groups = { "ONE", "TWO"};

            for (int msg = 0; msg < 4; msg++)
            {
                producer.send(createMessage(producerSession, msg, groups[msg % groups.length], useDefaultGroup));
            }
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            Session cs1 = consumerConnection.createSession(false, CLIENT_ACKNOWLEDGE);
            Session cs2 = consumerConnection.createSession(false, CLIENT_ACKNOWLEDGE);

            MessageConsumer consumer1 = cs1.createConsumer(queue);
            MessageConsumer consumer2 = cs2.createConsumer(queue);

            consumerConnection.start();
            Message cs1Received = consumer1.receive(getReceiveTimeout());
            assertNotNull("Consumer 1 should have received first message", cs1Received);

            Message cs2Received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received first message", cs2Received);

            cs2Received.acknowledge();

            Message cs2Received2 = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received second message", cs2Received2);
            assertEquals("Differing groups", cs2Received2.getStringProperty(groupKey),
                         cs2Received.getStringProperty(groupKey));

            cs1Received.acknowledge();
            Message cs1Received2 = consumer1.receive(getReceiveTimeout());

            assertNotNull("Consumer 1 should have received second message", cs1Received2);
            assertEquals("Differing groups", cs1Received2.getStringProperty(groupKey),
                         cs1Received.getStringProperty(groupKey));

            cs1Received2.acknowledge();
            cs2Received2.acknowledge();

            assertNull(consumer1.receive(getReceiveTimeout()));
            assertNull(consumer2.receive(getReceiveTimeout()));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void consumerCloseGroupAssignment() throws Exception
    {
        consumerCloseGroupAssignment(false, false);
    }

    @Test
    public void sharedGroupConsumerCloseGroupAssignment() throws Exception
    {
        consumerCloseGroupAssignment(true, false);
    }

    /**
     *
     * Tests that upon closing a consumer, groups previously assigned to that consumer are reassigned to a different
     * consumer.
     *
     * Pre-populate the queue as ONE, ONE, TWO, ONE
     *
     * create in sequence two consumers
     *
     * receive first from c1 then c2 (thus ONE is assigned to c1, TWO to c2)
     *
     * Then close c1 before acking.
     *
     * If we now attempt to receive from c2, then the remaining messages in group ONE should be available (which
     * requires c2 to go "backwards" in the queue).
     *
     **/
    private void consumerCloseGroupAssignment(boolean sharedGroups, final boolean useDefaultGroup) throws Exception
    {
        final String groupKey = getGroupKey(useDefaultGroup);
        String queueName = getTestName();
        Destination queue = createQueue(queueName, sharedGroups, false);

        final Connection producerConnection = getConnection();
        try
        {
            final Session producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", useDefaultGroup));
            producer.send(createMessage(producerSession, 2, "ONE", useDefaultGroup));
            producer.send(createMessage(producerSession, 3, "TWO", useDefaultGroup));
            producer.send(createMessage(producerSession, 4, "ONE", useDefaultGroup));
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            consumerConnection.start();
            Session cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            Session cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            MessageConsumer consumer1 = cs1.createConsumer(queue);
            MessageConsumer consumer2 = cs2.createConsumer(queue);

            Message cs1Received = consumer1.receive(getReceiveTimeout());
            assertNotNull("Consumer 1 should have received first message", cs1Received);
            assertEquals("incorrect message received", 1, cs1Received.getIntProperty("msg"));

            Message cs2Received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received first message", cs2Received);
            assertEquals("incorrect message received", 3, cs2Received.getIntProperty("msg"));
            cs2.commit();

            Message cs2Received2 = consumer2.receive(getReceiveTimeout());

            assertNull("Consumer 2 should not yet have received a second message", cs2Received2);

            consumer1.close();

            cs1.commit();
            Message cs2Received3 = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received second message", cs2Received3);
            assertEquals("Unexpected group",
                         "ONE",
                         cs2Received3.getStringProperty(groupKey));
            assertEquals("incorrect message received", 2, cs2Received3.getIntProperty("msg"));

            cs2.commit();

            Message cs2Received4 = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received third message", cs2Received4);
            assertEquals("Unexpected group",
                         "ONE",
                         cs2Received4.getStringProperty(groupKey));
            assertEquals("incorrect message received", 4, cs2Received4.getIntProperty("msg"));
            cs2.commit();

            assertNull(consumer2.receive(getReceiveTimeout()));
        }
        finally
        {
            consumerConnection.close();
        }
    }



    @Test
    public void consumerCloseWithRelease() throws Exception
    {
        consumerCloseWithRelease(false, false);
    }

    @Test
    public void sharedGroupConsumerCloseWithRelease() throws Exception
    {
        consumerCloseWithRelease(true, false);
    }

    /**
     *
     * Tests that upon closing a consumer and its session, groups previously assigned to that consumer are reassigned
     * to a different consumer, including messages which were previously delivered but have now been released.
     *
     * Pre-populate the queue as ONE, ONE, TWO, ONE
     *
     * create in sequence two consumers
     *
     * receive first from c1 then c2 (thus ONE is assigned to c1, TWO to c2)
     *
     * Then close c1 and its session without acking.
     *
     * If we now attempt to receive from c2, then the all messages in group ONE should be available (which
     * requires c2 to go "backwards" in the queue). The first such message should be marked as redelivered
     *
     */
    private void consumerCloseWithRelease(boolean sharedGroups, final boolean useDefaultGroup) throws Exception
    {
        String queueName = getTestName();
        final String groupKey = getGroupKey(useDefaultGroup);
        Destination queue = createQueue(queueName, sharedGroups, false);

        final Connection producerConnection = getConnection();
        try
        {
            producerConnection.start();
            final Session producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", useDefaultGroup));
            producer.send(createMessage(producerSession, 2, "ONE", useDefaultGroup));
            producer.send(createMessage(producerSession, 3, "TWO", useDefaultGroup));
            producer.send(createMessage(producerSession, 4, "ONE", useDefaultGroup));
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            consumerConnection.start();

            Session cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            Session cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            MessageConsumer consumer1 = cs1.createConsumer(queue);
            MessageConsumer consumer2 = cs2.createConsumer(queue);

            Message cs1Received = consumer1.receive(getReceiveTimeout());
            assertNotNull("Consumer 1 should have received its first message", cs1Received);
            assertEquals("incorrect message received", 1, cs1Received.getIntProperty("msg"));

            Message received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received its first message", received);
            assertEquals("incorrect message received", 3, received.getIntProperty("msg"));

            Message received2 = consumer2.receive(getReceiveTimeout());

            assertNull("Consumer 2 should not yet have received second message", received2);

            consumer1.close();
            cs1.close();
            cs2.commit();

            received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should now have received second message", received);
            assertEquals("Unexpected group",
                         "ONE",
                         received.getStringProperty(groupKey));
            assertEquals("incorrect message received", 1, received.getIntProperty("msg"));
            assertTrue("Expected second message to be marked as redelivered " + received.getIntProperty("msg"),
                       received.getJMSRedelivered());

            cs2.commit();

            received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received a third message", received);
            assertEquals("Unexpected group",
                         "ONE",
                         received.getStringProperty(groupKey));
            assertEquals("incorrect message received", 2, received.getIntProperty("msg"));

            cs2.commit();

            received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received a fourth message", received);
            assertEquals("Unexpected group",
                         "ONE",
                         received.getStringProperty(groupKey));
            assertEquals("incorrect message received", 4, received.getIntProperty("msg"));

            cs2.commit();

            assertNull(consumer2.receive(getReceiveTimeout()));
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void groupAssignmentSurvivesEmpty() throws Exception
    {
        groupAssignmentOnEmpty(false, false);
    }

    @Test
    public void sharedGroupAssignmentDoesNotSurviveEmpty() throws Exception
    {
        groupAssignmentOnEmpty(true, false);
    }

    private void groupAssignmentOnEmpty(boolean sharedGroups, final boolean useDefaultGroup) throws Exception
    {
        String queueName = getTestName();
        Destination queue = createQueue(queueName, sharedGroups, false);

        final Connection producerConnection = getConnection();
        try
        {
            producerConnection.start();
            final Session producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
            final MessageProducer producer = producerSession.createProducer(queue);

            producer.send(createMessage(producerSession, 1, "ONE", useDefaultGroup));
            producer.send(createMessage(producerSession, 2, "TWO", useDefaultGroup));
            producer.send(createMessage(producerSession, 3, "THREE", useDefaultGroup));
            producer.send(createMessage(producerSession, 4, "ONE", useDefaultGroup));
            producerSession.commit();
        }
        finally
        {
            producerConnection.close();
        }

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(0).build();
        try
        {
            consumerConnection.start();

            Session cs1 = consumerConnection.createSession(true, SESSION_TRANSACTED);
            Session cs2 = consumerConnection.createSession(true, SESSION_TRANSACTED);

            MessageConsumer consumer1 = cs1.createConsumer(queue);
            MessageConsumer consumer2 = cs2.createConsumer(queue);

            Message cs1Received = consumer1.receive(getReceiveTimeout());
            assertNotNull("Consumer 1 should have received its first message", cs1Received);
            assertEquals("incorrect message received", 1, cs1Received.getIntProperty("msg"));

            Message cs2Received = consumer2.receive(getReceiveTimeout());

            assertNotNull("Consumer 2 should have received its first message", cs2Received);
            assertEquals("incorrect message received", 2, cs2Received.getIntProperty("msg"));

            cs1.commit();

            cs1Received = consumer1.receive(getReceiveTimeout());
            assertNotNull("Consumer 1 should have received its second message", cs1Received);
            assertEquals("incorrect message received", 3, cs1Received.getIntProperty("msg"));

            // We expect different behaviours from "shared groups": here the assignment of a subscription to a group
            // is terminated when there are no outstanding delivered but unacknowledged messages.  In contrast, with a
            // standard message grouping queue the assignment will be retained until the subscription is no longer
            // registered
            if (sharedGroups)
            {
                cs2.commit();
                cs2Received = consumer2.receive(getReceiveTimeout());

                assertNotNull("Consumer 2 should have received its second message", cs2Received);
                assertEquals("incorrect message received", 4, cs2Received.getIntProperty("msg"));

                cs2.commit();
            }
            else
            {
                cs2.commit();
                cs2Received = consumer2.receive(getReceiveTimeout());

                assertNull("Consumer 2 should not have received a second message", cs2Received);

                cs1.commit();

                cs1Received = consumer1.receive(getReceiveTimeout());
                assertNotNull("Consumer 1 should have received its third message", cs1Received);
                assertEquals("incorrect message received", 4, cs1Received.getIntProperty("msg"));
            }
        }
        finally
        {
            consumerConnection.close();
        }

    }

    /**
     * Tests that when a number of new messages for a given groupid are arriving while the delivery group
     * state is also in the process of being emptied (due to acking a message while using prefetch=1), that only
     * 1 of a number of existing consumers is ever receiving messages for the shared group at a time.
     */
    @Test
    public void singleSharedGroupWithMultipleConsumers() throws Exception
    {
        String queueName = getTestName();
        final boolean useDefaultGroup = false;
        Destination queue = createQueue(queueName, true, useDefaultGroup);

        final Connection consumerConnection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            int numMessages = 100;
            SharedGroupTestMessageListener groupingTestMessageListener =
                    new SharedGroupTestMessageListener(numMessages);

            Session cs1 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            Session cs2 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            Session cs3 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);
            Session cs4 = consumerConnection.createSession(false, AUTO_ACKNOWLEDGE);

            MessageConsumer consumer1 = cs1.createConsumer(queue);
            consumer1.setMessageListener(groupingTestMessageListener);
            MessageConsumer consumer2 = cs2.createConsumer(queue);
            consumer2.setMessageListener(groupingTestMessageListener);
            MessageConsumer consumer3 = cs3.createConsumer(queue);
            consumer3.setMessageListener(groupingTestMessageListener);
            MessageConsumer consumer4 = cs4.createConsumer(queue);
            consumer4.setMessageListener(groupingTestMessageListener);
            consumerConnection.start();

            final Connection producerConnection = getConnection();
            try
            {
                final Session producerSession = producerConnection.createSession(true, SESSION_TRANSACTED);
                final MessageProducer producer = producerSession.createProducer(queue);

                for (int i = 1; i <= numMessages; i++)
                {
                    producer.send(createMessage(producerSession, i, "GROUP", useDefaultGroup));
                }

                producerSession.commit();
            }
            finally
            {
                producerConnection.close();
            }

            assertTrue("Mesages not all received in the allowed timeframe",
                       groupingTestMessageListener.waitForLatch(30));
            assertEquals("Unexpected concurrent processing of messages for the group",
                         0,
                         groupingTestMessageListener.getConcurrentProcessingCases());
            assertNull("Unexpected throwable in message listeners", groupingTestMessageListener.getThrowable());
        }
        finally
        {
            consumerConnection.close();
        }
    }

    private Destination createQueue(final String queueName, final boolean sharedGroups,
                                    final boolean useDefaultKey) throws Exception
    {
        final Map<String, Object> arguments = new HashMap<>();
        if(!useDefaultKey)
        {
            arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_GROUP_KEY_OVERRIDE, "group");
        }
        arguments.put(ConfiguredObject.DURABLE, "false");
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_GROUP_TYPE, sharedGroups ? MessageGroupType.SHARED_GROUPS.name() : MessageGroupType.STANDARD.name());

        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", arguments);

        return createQueue(queueName);
    }

    private Message createMessage(final Session session, int msg, String group, final boolean useDefaultGroup) throws JMSException
    {
        Message send = session.createTextMessage("Message: " + msg);
        send.setIntProperty("msg", msg);
        send.setStringProperty(getGroupKey(useDefaultGroup), group);

        return send;
    }

    private String getGroupKey(final boolean useDefaultGroup)
    {
        return useDefaultGroup ? "JMSXGroupID" : "group";
    }

    public static class SharedGroupTestMessageListener implements MessageListener
    {

        private final CountDownLatch _count;
        private final AtomicInteger _activeListeners = new AtomicInteger();
        private final AtomicInteger _concurrentProcessingCases = new AtomicInteger();
        private Throwable _throwable;

        SharedGroupTestMessageListener(int numMessages)
        {
            _count = new CountDownLatch(numMessages);
        }

        @Override
        public void onMessage(Message message)
        {
            try
            {
                int currentActiveListeners = _activeListeners.incrementAndGet();

                if (currentActiveListeners > 1)
                {
                    _concurrentProcessingCases.incrementAndGet();

                    LOGGER.error("Concurrent processing when handling message: " + message.getIntProperty("msg"));
                }

                try
                {
                    Thread.sleep(25);
                }
                catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                }

                _activeListeners.decrementAndGet();
            }
            catch (Throwable t)
            {
                LOGGER.error("Unexpected throwable received by listener", t);
                _throwable = t;
            }
            finally
            {
                _count.countDown();
            }
        }

        boolean waitForLatch(int seconds) throws Exception
        {
            return _count.await(seconds, TimeUnit.SECONDS);
        }

        int getConcurrentProcessingCases()
        {
            return _concurrentProcessingCases.get();
        }

        Throwable getThrowable()
        {
            return _throwable;
        }
    }
}
