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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.autocreation;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.AlternateBinding;
import org.apache.qpid.server.model.Exchange;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.junit.Test;

/**
 * NodeAutoCreationPolicyTest.
 */
public class NodeAutoCreationPolicyTest extends JmsTestBase
{
    private static final String DEAD_LETTER_QUEUE_SUFFIX = "_DLQ";
    private static final String DEAD_LETTER_EXCHANGE_SUFFIX = "_DLE";
    private static final String AUTO_CREATION_POLICIES = createAutoCreationPolicies();
    private static final String TEST_MESSAGE = "Hello world!";
    private static final String VALID_QUEUE_NAME = "fooQueue";
    private static final String TYPE_QUEUE = "queue";
    private static final String TYPE_TOPIC = "topic";


    private static String createAutoCreationPolicies()
    {
        ObjectMapper mapper = new ObjectMapper();
        try
        {

            NodeAutoCreationPolicy[] policies = new NodeAutoCreationPolicy[] {
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "fooQ.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return true;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Queue";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.emptyMap();
                        }
                    },
                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return "barE.*";
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return false;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Exchange";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.singletonMap(Exchange.TYPE, "fanout");
                        }
                    },

                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return ".*" + DEAD_LETTER_QUEUE_SUFFIX;
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return true;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Queue";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.emptyMap();
                        }
                    },

                    new NodeAutoCreationPolicy()
                    {
                        @Override
                        public String getPattern()
                        {
                            return ".*" + DEAD_LETTER_EXCHANGE_SUFFIX;
                        }

                        @Override
                        public boolean isCreatedOnPublish()
                        {
                            return true;
                        }

                        @Override
                        public boolean isCreatedOnConsume()
                        {
                            return false;
                        }

                        @Override
                        public String getNodeType()
                        {
                            return "Exchange";
                        }

                        @Override
                        public Map<String, Object> getAttributes()
                        {
                            return Collections.singletonMap(Exchange.TYPE, ExchangeDefaults.FANOUT_EXCHANGE_CLASS);
                        }
                    }
            };

            return mapper.writeValueAsString(Arrays.asList(policies));
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void updateAutoCreationPolicies() throws Exception
    {
        updateEntityUsingAmqpManagement(getVirtualHostName(), "org.apache.qpid.VirtualHost", Collections.singletonMap(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, AUTO_CREATION_POLICIES));
    }

    @Test
    public void testSendingToQueuePattern() throws Exception
    {
        updateAutoCreationPolicies();

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(getDestinationAddress(VALID_QUEUE_NAME, TYPE_QUEUE));
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(TEST_MESSAGE));

            final MessageConsumer consumer = session.createConsumer(queue);
            Message received = consumer.receive(getReceiveTimeout());
            assertNotNull(received);
            assertTrue(received instanceof TextMessage);
            assertEquals(TEST_MESSAGE, ((TextMessage) received).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConcurrentQueueCreation() throws Exception
    {
        updateAutoCreationPolicies();

        final String destination = getDestinationAddress(VALID_QUEUE_NAME, TYPE_QUEUE);
        final int numberOfActors = 3;
        final Connection[] connections = new Connection[numberOfActors];
        try
        {
            final Session[] sessions = new Session[numberOfActors];
            for (int i = 0; i < numberOfActors; i++)
            {
                final Connection connection = getConnection();
                connections[i] = connection;
                sessions[i] = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }

            final List<CompletableFuture<MessageProducer>> futures = new ArrayList<>(numberOfActors);
            final ExecutorService executorService = Executors.newFixedThreadPool(numberOfActors);
            try
            {
                Stream.of(sessions)
                      .forEach(session -> futures.add(CompletableFuture.supplyAsync(() -> publishMessage(session,
                                                                                                         destination),
                                                                                    executorService)));
                final CompletableFuture<Void> combinedFuture =
                        CompletableFuture.allOf(futures.toArray(new CompletableFuture[numberOfActors]));
                combinedFuture.get(getReceiveTimeout(), TimeUnit.MILLISECONDS);
            }
            finally
            {
                executorService.shutdown();
            }

            final Connection connection = getConnection();
            try
            {
                connection.start();
                final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final Queue queue = session.createQueue(destination);
                final MessageConsumer consumer = session.createConsumer(queue);

                for (int i = 0; i < numberOfActors; i++)
                {
                    Message received = consumer.receive(getReceiveTimeout());
                    assertNotNull(received);
                    assertTrue(received instanceof TextMessage);
                    assertEquals(TEST_MESSAGE, ((TextMessage) received).getText());
                }
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            for (Connection connection : connections)
            {
                if (connection != null)
                {
                    connection.close();
                }
            }
        }
    }

    private MessageProducer publishMessage(final Session session, final String destination)
    {
        try
        {
            final Queue queue = session.createQueue(destination);
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(TEST_MESSAGE));
            return producer;
        }
        catch (JMSException e)
        {
            throw new IllegalStateException(e);
        }
    }

    @Test
    public void testSendingToNonMatchingQueuePattern() throws Exception
    {
        updateAutoCreationPolicies();

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(getDestinationAddress("foQueue", TYPE_QUEUE));
            try
            {
                session.createProducer(queue);
                fail("Creating producer should fail");
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

    @Test
    public void testSendingToExchangePattern() throws Exception
    {
        updateAutoCreationPolicies();

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(getDestinationAddress("barExchange/foo", TYPE_TOPIC));
            final MessageProducer producer = session.createProducer(topic);
            producer.send(session.createTextMessage(TEST_MESSAGE));

            final MessageConsumer consumer = session.createConsumer(topic);
            Message received = consumer.receive(getReceiveTimeout() / 4);
            assertNull(received);

            producer.send(session.createTextMessage("Hello world2!"));
            received = consumer.receive(getReceiveTimeout());

            assertNotNull(received);

            assertTrue(received instanceof TextMessage);
            assertEquals("Hello world2!", ((TextMessage) received).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSendingToNonMatchingTopicPattern() throws Exception
    {
        updateAutoCreationPolicies();

        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(getDestinationAddress("baa", TYPE_TOPIC));
            try
            {
                session.createProducer(topic);
                fail("Creating producer should fail");
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

    @Test
    public void testSendingToQueuePatternBURL() throws Exception
    {
        assumeThat("Qpid JMS Client does not support BURL syntax",
                   getProtocol(),
                   is(not(equalTo(Protocol.AMQP_1_0))));
        updateAutoCreationPolicies();

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("BURL:direct:///fooQ/fooQ");
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage(TEST_MESSAGE));

            final MessageConsumer consumer = session.createConsumer(queue);
            Message received = consumer.receive(getReceiveTimeout());
            assertNotNull(received);
            assertTrue(received instanceof TextMessage);
            assertEquals(TEST_MESSAGE, ((TextMessage) received).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSendingToNonMatchingQueuePatternBURL() throws Exception
    {
        assumeThat("Using AMQP 0-8..0-9-1 to test BURL syntax",
                   getProtocol(),
                   is(not(anyOf(equalTo(Protocol.AMQP_1_0), equalTo(Protocol.AMQP_0_10)))));
        updateAutoCreationPolicies();

        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue("BURL:direct:///fo/fo");
            try
            {
                final MessageProducer producer = session.createProducer(queue);
                producer.send(session.createTextMessage(TEST_MESSAGE));

                fail("Sending a message should fail");
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

    @Test
    public void testQueueAlternateBindingCreation() throws Exception
    {
        updateAutoCreationPolicies();

        String queueName = getTestName();
        String deadLetterQueueName = queueName + DEAD_LETTER_QUEUE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> expectedAlternateBinding =
                Collections.singletonMap(AlternateBinding.DESTINATION, deadLetterQueueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(expectedAlternateBinding));
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", attributes);

        Map<String, Object> queueAttributes = readEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", true);

        Object actualAlternateBinding = queueAttributes.get(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING);
        Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals("Unexpected alternate binding",
                     new HashMap<>(expectedAlternateBinding),
                     new HashMap<>(actualAlternateBindingMap));

        Map<String, Object> dlqAttributes =
                readEntityUsingAmqpManagement(deadLetterQueueName, "org.apache.qpid.Queue", true);
        assertNotNull("Cannot get dead letter queue", dlqAttributes);
    }

    @Test
    public void testExchangeAlternateBindingCreation() throws Exception
    {
        updateAutoCreationPolicies();

        String exchangeName = getTestName();
        String deadLetterExchangeName = exchangeName + DEAD_LETTER_EXCHANGE_SUFFIX;

        final Map<String, Object> attributes = new HashMap<>();
        Map<String, Object> expectedAlternateBinding =
                Collections.singletonMap(AlternateBinding.DESTINATION, deadLetterExchangeName);
        attributes.put(Exchange.ALTERNATE_BINDING, new ObjectMapper().writeValueAsString(expectedAlternateBinding));
        attributes.put(Exchange.TYPE, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
        createEntityUsingAmqpManagement(exchangeName, "org.apache.qpid.DirectExchange", attributes);

        Map<String, Object> exchangeAttributes = readEntityUsingAmqpManagement(exchangeName, "org.apache.qpid.Exchange", true);

        Object actualAlternateBinding = exchangeAttributes.get(Exchange.ALTERNATE_BINDING);
        Map<String, Object> actualAlternateBindingMap = convertIfNecessary(actualAlternateBinding);
        assertEquals("Unexpected alternate binding",
                     new HashMap<>(expectedAlternateBinding),
                     new HashMap<>(actualAlternateBindingMap));

        Map<String, Object> dlqExchangeAttributes = readEntityUsingAmqpManagement(
                deadLetterExchangeName,
                "org.apache.qpid.FanoutExchange",
                true);
        assertNotNull("Cannot get dead letter exchange", dlqExchangeAttributes);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertIfNecessary(final Object actualAlternateBinding) throws IOException
    {
        Map<String, Object> actualAlternateBindingMap;
        if (actualAlternateBinding instanceof String)
        {
            actualAlternateBindingMap = new ObjectMapper().readValue((String)actualAlternateBinding, Map.class);
        }
        else
        {
            actualAlternateBindingMap = (Map<String, Object>) actualAlternateBinding;
        }
        return actualAlternateBindingMap;
    }

    private String getDestinationAddress(final String name, final String type)
    {
        return getProtocol() == Protocol.AMQP_1_0
                ? name
                : String.format("ADDR: %s; { assert: never, node: { type: %s } }", name, type);
    }
}
