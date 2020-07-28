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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.AmqpManagementFacade;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.store.MessageDurability;
import org.junit.Ignore;
import org.junit.Test;

/**
 * QueueMessageDurabilityTest.
 */
@Ignore
public class QueueMessageDurabilityTest extends JmsTestBase
{
    private static final String DURABLE_ALWAYS_PERSIST_NAME = "DURABLE_QUEUE_ALWAYS_PERSIST";
    private static final String DURABLE_NEVER_PERSIST_NAME = "DURABLE_QUEUE_NEVER_PERSIST";
    private static final String DURABLE_DEFAULT_PERSIST_NAME = "DURABLE_QUEUE_DEFAULT_PERSIST";
    private static final String NONDURABLE_ALWAYS_PERSIST_NAME = "NONDURABLE_QUEUE_ALWAYS_PERSIST";

    @Test
    public void testSendPersistentMessageToAll() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        prepare();

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(null);
            producer.send(session.createTopic(getTestTopic("Y.Y.Y.Y")), session.createTextMessage("test"));
            session.commit();
        }
        finally
        {
            connection.close();
        }

        assertEquals(1, getQueueDepth(DURABLE_NEVER_PERSIST_NAME));
        assertEquals(1, getQueueDepth(NONDURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_DEFAULT_PERSIST_NAME));

        getBrokerAdmin().restart();

        assertEquals(0, getQueueDepth(DURABLE_NEVER_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_DEFAULT_PERSIST_NAME));
        assertFalse(isQueueExist(NONDURABLE_ALWAYS_PERSIST_NAME));
    }

    @Test
    public void testSendNonPersistentMessageToAll() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        prepare();

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            connection.start();
            producer.send(session.createTopic(getTestTopic("Y.Y.Y.Y")), session.createTextMessage("test"));
            session.commit();
        }
        finally
        {
            connection.close();
        }

        assertEquals(1, getQueueDepth(DURABLE_NEVER_PERSIST_NAME));
        assertEquals(1, getQueueDepth(NONDURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_DEFAULT_PERSIST_NAME));

        getBrokerAdmin().restart();

        assertEquals(0, getQueueDepth(DURABLE_NEVER_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(0, getQueueDepth(DURABLE_DEFAULT_PERSIST_NAME));
        assertFalse(isQueueExist(NONDURABLE_ALWAYS_PERSIST_NAME));
    }

    @Test
    public void testNonPersistentContentRetained() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        prepare();

        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.send(session.createTopic(getTestTopic("N.N.Y.Y")), session.createTextMessage("test1"));
            producer.send(session.createTopic(getTestTopic("Y.N.Y.Y")), session.createTextMessage("test2"));
            session.commit();

            MessageConsumer consumer = session.createConsumer(session.createQueue(DURABLE_ALWAYS_PERSIST_NAME));
            Message msg = consumer.receive(getReceiveTimeout());
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("test2", ((TextMessage) msg).getText());
            session.rollback();
        }
        finally
        {
            connection.close();
        }

        getBrokerAdmin().restart();

        assertEquals(0, getQueueDepth(DURABLE_NEVER_PERSIST_NAME));
        assertEquals(1, getQueueDepth(DURABLE_ALWAYS_PERSIST_NAME));
        assertEquals(0, getQueueDepth(DURABLE_DEFAULT_PERSIST_NAME));

        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            Session session = connection2.createSession(true, Session.SESSION_TRANSACTED);

            MessageConsumer consumer = session.createConsumer(session.createQueue(DURABLE_ALWAYS_PERSIST_NAME));
            Message msg = consumer.receive(getReceiveTimeout());
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("test2", ((TextMessage) msg).getText());
            session.commit();
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void testPersistentContentRetainedOnTransientQueue() throws Exception
    {
        prepare();

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(null);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            producer.send(session.createTopic(getTestTopic("N.N.Y.Y")), session.createTextMessage("test1"));
            session.commit();
            MessageConsumer consumer =
                    session.createConsumer(session.createQueue(getTestQueue(DURABLE_DEFAULT_PERSIST_NAME)));
            Message msg = consumer.receive(getReceiveTimeout());
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("test1", ((TextMessage) msg).getText());
            session.commit();

            consumer = session.createConsumer(session.createQueue(getTestQueue(NONDURABLE_ALWAYS_PERSIST_NAME)));
            msg = consumer.receive(getReceiveTimeout());
            assertNotNull(msg);
            assertTrue(msg instanceof TextMessage);
            assertEquals("test1", ((TextMessage) msg).getText());
            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    private boolean isQueueExist(final String queueName) throws Exception
    {
        try
        {
            performOperationUsingAmqpManagement(queueName,
                                                "READ",
                                                "org.apache.qpid.Queue",
                                                Collections.emptyMap());
            return true;
        }
        catch (AmqpManagementFacade.OperationUnsuccessfulException e)
        {
            if (e.getStatusCode() == 404)
            {
                return false;
            }
            else
            {
                throw e;
            }
        }
    }

    private int getQueueDepth(final String queueName) throws Exception
    {
        Map<String, Object> arguments =
                Collections.singletonMap("statistics", Collections.singletonList("queueDepthMessages"));
        Object statistics = performOperationUsingAmqpManagement(queueName,
                                                                "getStatistics",
                                                                "org.apache.qpid.Queue",
                                                                arguments);
        assertNotNull("Statistics is null", statistics);
        assertTrue("Statistics is not map", statistics instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        assertTrue("queueDepthMessages is not present", statisticsMap.get("queueDepthMessages") instanceof Number);
        return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
    }

    private void prepare() throws Exception
    {
        Map<String, Object> arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        createEntityUsingAmqpManagement(DURABLE_ALWAYS_PERSIST_NAME, "org.apache.qpid.Queue", arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.NEVER.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        createEntityUsingAmqpManagement(DURABLE_NEVER_PERSIST_NAME, "org.apache.qpid.Queue", arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.DEFAULT.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, true);
        createEntityUsingAmqpManagement(DURABLE_DEFAULT_PERSIST_NAME, "org.apache.qpid.Queue", arguments);

        arguments = new HashMap<>();
        arguments.put(org.apache.qpid.server.model.Queue.MESSAGE_DURABILITY, MessageDurability.ALWAYS.name());
        arguments.put(org.apache.qpid.server.model.Queue.DURABLE, false);
        createEntityUsingAmqpManagement(NONDURABLE_ALWAYS_PERSIST_NAME, "org.apache.qpid.Queue", arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_ALWAYS_PERSIST_NAME);
        arguments.put("bindingKey", "Y.*.*.*");
        performOperationUsingAmqpManagement("amq.topic", "bind", "org.apache.qpid.TopicExchange", arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_NEVER_PERSIST_NAME);
        arguments.put("bindingKey", "*.Y.*.*");
        performOperationUsingAmqpManagement("amq.topic", "bind", "org.apache.qpid.TopicExchange", arguments);

        arguments = new HashMap<>();
        arguments.put("destination", DURABLE_DEFAULT_PERSIST_NAME);
        arguments.put("bindingKey", "*.*.Y.*");
        performOperationUsingAmqpManagement("amq.topic", "bind", "org.apache.qpid.TopicExchange", arguments);

        arguments = new HashMap<>();
        arguments.put("destination", NONDURABLE_ALWAYS_PERSIST_NAME);
        arguments.put("bindingKey", "*.*.*.Y");
        performOperationUsingAmqpManagement("amq.topic", "bind", "org.apache.qpid.TopicExchange", arguments);
    }

    private String getTestTopic(final String routingKey)
    {
        String topicNameFormat = getProtocol() == Protocol.AMQP_1_0 ? "amq.topic/%s" : "%s";
        return String.format(topicNameFormat, routingKey);
    }

    private String getTestQueue(final String name)
    {
        String queueFormat =
                getProtocol() == Protocol.AMQP_1_0 ? "%s" : "ADDR:%s; {create:never, node: { type: queue }}";
        return String.format(queueFormat, name);
    }
}
