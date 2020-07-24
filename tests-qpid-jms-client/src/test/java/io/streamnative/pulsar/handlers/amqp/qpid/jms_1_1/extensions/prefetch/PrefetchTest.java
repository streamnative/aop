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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.prefetch;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static junit.framework.TestCase.assertEquals;
import static org.apache.qpid.server.model.Protocol.AMQP_0_8;
import static org.apache.qpid.server.model.Protocol.AMQP_0_9;
import static org.apache.qpid.server.model.Protocol.AMQP_0_9_1;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.EnumSet;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * PrefetchTest.
 */
public class PrefetchTest extends JmsTestBase
{
    private static final EnumSet<Protocol> PRE_010_PROTOCOLS = EnumSet.of(AMQP_0_8, AMQP_0_9, AMQP_0_9_1);

    @Test
    public void prefetch() throws Exception
    {
        Connection connection1 = getConnectionBuilder().setPrefetch(3).build();
        Queue queue = createQueue(getTestName());
        try
        {
            connection1.start();

            final Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session1.createConsumer(queue);

            Utils.sendMessages(connection1, queue, 6);

            final Message receivedMessage = consumer1.receive(getReceiveTimeout());
            assertNotNull("First message was not received", receivedMessage);
            assertEquals("Received message has unexpected index", 0, receivedMessage.getIntProperty(INDEX));

            forceSync(session1);

            observeNextAvailableMessage(queue, 4);
        }
        finally
        {
            connection1.close();
        }
    }

    /**
     * send two messages to the queue, consume and acknowledge one message on connection 1
     * create a second connection and attempt to consume the second message - this will only be possible
     * if the first connection has no prefetch
     */
    @Test
    public void prefetchDisabled() throws Exception
    {
        Connection connection1 = getConnectionBuilder().setPrefetch(0).build();
        Queue queue = createQueue(getTestName());
        try
        {
            connection1.start();

            final Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session1.createConsumer(queue);

            Utils.sendMessages(connection1, queue, 2);

            final Message receivedMessage = consumer1.receive(getReceiveTimeout());
            assertNotNull("First message was not received", receivedMessage);
            assertEquals("Message property was not as expected", 0, receivedMessage.getIntProperty(INDEX));

            observeNextAvailableMessage(queue, 1);
        }
        finally
        {
            connection1.close();
        }
    }

    @Test
    public void connectionStopReleasesPrefetchedMessages() throws Exception
    {
        assumeThat("Only 0-10 implements this feature", getProtocol(), is(equalTo(Protocol.AMQP_0_10)));

        Connection connection1 = getConnectionBuilder().setPrefetch(3).build();
        Queue queue = createQueue(getTestName());
        try
        {
            connection1.start();

            final Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session1.createConsumer(queue);

            Utils.sendMessages(connection1, queue, 6);

            final Message receivedMessage = consumer1.receive(getReceiveTimeout());
            assertNotNull("First message was not received", receivedMessage);
            assertEquals("Received message has unexpected index", 0, receivedMessage.getIntProperty(INDEX));

            forceSync(session1);

            connection1.stop();

            observeNextAvailableMessage(queue, 1);
        }
        finally
        {
            connection1.close();
        }
    }

    @Test
    public void consumerCloseReleasesPrefetchedMessages() throws Exception
    {
        assumeThat("Only 0-10 implements this feature", getProtocol(), is(equalTo(Protocol.AMQP_0_10)));

        Connection connection1 = getConnectionBuilder().setPrefetch(3).build();
        Queue queue = createQueue(getTestName());
        try
        {
            connection1.start();

            final Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = session1.createConsumer(queue);

            Utils.sendMessages(connection1, queue, 6);

            final Message receivedMessage = consumer1.receive(getReceiveTimeout());
            assertNotNull("First message was not received", receivedMessage);
            assertEquals("Received message has unexpected index", 0, receivedMessage.getIntProperty(INDEX));

            forceSync(session1);

            consumer1.close();

            observeNextAvailableMessage(queue, 1);
        }
        finally
        {
            connection1.close();
        }
    }

    @Test
    public void consumeBeyondPrefetch() throws Exception
    {
        Connection connection1 = getConnectionBuilder().setPrefetch(1).build();
        Queue queue = createQueue(getTestName());
        try
        {
            connection1.start();

            final Session session1 = connection1.createSession(true, Session.SESSION_TRANSACTED);
            MessageConsumer consumer1 = session1.createConsumer(queue);

            Utils.sendMessages(connection1, queue, 5);

            Message message = consumer1.receive(getReceiveTimeout());
            assertNotNull(message);
            assertEquals(0, message.getIntProperty(INDEX));

            message = consumer1.receive(getReceiveTimeout());
            assertNotNull(message);
            assertEquals(1, message.getIntProperty(INDEX));
            message = consumer1.receive(getReceiveTimeout());
            assertNotNull(message);
            assertEquals(2, message.getIntProperty(INDEX));

            forceSync(session1);

            // In pre 0-10, in a transaction session the client does not ack the message until the commit occurs
            // so the message observed by another connection will have the index 3 rather than 4.
            Connection connection2 = getConnection();
            try
            {
                Session session2 = connection2.createSession(true, Session.SESSION_TRANSACTED);
                MessageConsumer consumer2 = session2.createConsumer(queue);
                connection2.start();

                message = consumer2.receive(getReceiveTimeout());
                assertNotNull(message);
                assertEquals("Received message has unexpected index",
                             PRE_010_PROTOCOLS.contains(getProtocol()) ? 3 : 4,
                             message.getIntProperty(INDEX));

                session2.rollback();
            }
            finally
            {
                connection2.close();
            }
        }
        finally
        {
            connection1.close();
        }
    }

    private void observeNextAvailableMessage(final Queue queue, final int expectedIndex) throws JMSException, NamingException
    {
        Connection connection2 = getConnection();
        try
        {
            connection2.start();
            final Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer2 = session2.createConsumer(queue);

            final Message receivedMessage2 = consumer2.receive(getReceiveTimeout());
            assertNotNull("Observer connection did not receive message", receivedMessage2);
            assertEquals("Message received by the observer connection has unexpected index", expectedIndex, receivedMessage2.getIntProperty(INDEX));
        }
        finally
        {
            connection2.close();
        }
    }

    private void forceSync(final Session session1) throws Exception
    {
        session1.createTemporaryQueue().delete();
    }
}
