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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.filters;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * ArrivalTimeFilterTest.
 */
public class ArrivalTimeFilterTest extends JmsTestBase
{
    private static final String LEGACY_BINDING_URL = "direct://amq.direct/%s/%s?x-qpid-replay-period='%d'";

    @Test
    public void testQueueDefaultFilterArrivalTime0() throws Exception
    {
        final String queueName = getTestName();
        createDestinationWithFilter(0, queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = getQueue(queueName);
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("B"));

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should be received", receivedMessage);
            assertTrue("Unexpected message type", receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message", "B", ((TextMessage) receivedMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testQueueDefaultFilterArrivalTime1000() throws Exception
    {
        final String queueName = getTestName();
        final long period = getReceiveTimeout() / 1000;
        createDestinationWithFilter(period, queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = getQueue(queueName);
            connection.start();
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            Thread.sleep(getReceiveTimeout() / 4);

            final MessageConsumer consumer = session.createConsumer(queue);

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message A should be received", receivedMessage);
            assertTrue("Unexpected message type", receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message", "A", ((TextMessage) receivedMessage).getText());

            producer.send(session.createTextMessage("B"));

            final Message secondMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message B should be received", secondMessage);
            assertTrue("Unexpected message type", secondMessage instanceof TextMessage);
            assertEquals("Unexpected message", "B", ((TextMessage) secondMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumerFilterArrivalTime0() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        createQueue(queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = session.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, 0));
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            producer.send(session.createTextMessage("B"));

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message should be received", receivedMessage);
            assertTrue("Unexpected message type", receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message", "B", ((TextMessage) receivedMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testConsumerFilterArrivalTime1000() throws Exception
    {
        assumeThat("Only legacy client implements this feature", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));

        final String queueName = getTestName();
        createQueue(queueName);
        final Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue =
                    session.createQueue(String.format(LEGACY_BINDING_URL, queueName, queueName, getReceiveTimeout()));
            connection.start();
            final MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));

            Thread.sleep(getReceiveTimeout() / 4);

            final MessageConsumer consumer = session.createConsumer(queue);

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message A should be received", receivedMessage);
            assertTrue("Unexpected message type", receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message", "A", ((TextMessage) receivedMessage).getText());

            producer.send(session.createTextMessage("B"));

            final Message secondMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message B should be received", secondMessage);
            assertTrue("Unexpected message type", secondMessage instanceof TextMessage);
            assertEquals("Unexpected message", "B", ((TextMessage) secondMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    private void createDestinationWithFilter(final long period, final String queueName) throws Exception
    {
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("defaultFilters",
                                                                 "{ \"x-qpid-replay-period\" : { \"x-qpid-replay-period\" : [ \""
                                                                 + period
                                                                 + "\" ] } }"));
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", queueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement("amq.direct",
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            arguments);
    }
}
