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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.acknowledge;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;

import org.junit.Ignore;
import org.junit.Test;

/**
 * AcknowledgeTest.
 */
public class AcknowledgeTest extends JmsTestBase
{
    private static final int TIMEOUT = 30000;

    @Test
    public void autoAcknowledgement() throws Exception
    {
        doAcknowledgementTest(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    public void dupsOkAcknowledgement() throws Exception
    {
        doAcknowledgementTest(Session.DUPS_OK_ACKNOWLEDGE);
    }

    @Test
    public void clientAcknowledgement() throws Exception
    {
        doAcknowledgementTest(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    @Ignore
    public void sessionTransactedAcknowledgement() throws Exception
    {
        doMessageListenerAcknowledgementTest(Session.SESSION_TRANSACTED);
    }

    @Test
    public void autoAcknowledgementMessageListener() throws Exception
    {
        doAcknowledgementTest(Session.AUTO_ACKNOWLEDGE);
    }

    @Test
    @Ignore
    public void clientAcknowledgementMessageListener() throws Exception
    {
        doMessageListenerAcknowledgementTest(Session.CLIENT_ACKNOWLEDGE);
    }

    @Test
    @Ignore
    public void sessionTransactedAcknowledgementMessageListener() throws Exception
    {
        doMessageListenerAcknowledgementTest(Session.SESSION_TRANSACTED);
    }

    private void doAcknowledgementTest(final int ackMode) throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Utils.sendMessages(session, queue, 2);

            Message message = consumer.receive(getReceiveTimeout());
            assertNotNull("First message has not been received", message);
            assertEquals("Unexpected message index received", 0, message.getIntProperty(INDEX));

            acknowledge(ackMode, session, message);

            message = consumer.receive(getReceiveTimeout());
            assertNotNull("Second message has not been received", message);
            assertEquals("Unexpected message index received", 1, message.getIntProperty(INDEX));
        }
        finally
        {
            connection.close();
        }

        verifyLeftOvers(ackMode, queue);
    }

    private void doMessageListenerAcknowledgementTest(final int ackMode) throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);
            connection.start();

            List<Integer> messageIndices = IntStream.rangeClosed(0, 2)
                                                    .boxed().collect(Collectors.toList());

            messageIndices.forEach(integer -> {
                try
                {
                    Message message = session.createMessage();
                    message.setIntProperty(INDEX, integer);
                    producer.send(message);
                }
                catch (JMSException e)
                {
                    throw new RuntimeException(e);
                }
            });

            if (session.getTransacted())
            {
                session.commit();
            }

            Set<Integer> pendingMessages = Sets.newHashSet(messageIndices);
            AtomicReference<Exception> exception = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(1);

            consumer.setMessageListener(message -> {
                try
                {
                    Object index = message.getObjectProperty(INDEX);
                    boolean removed = index instanceof Integer && pendingMessages.remove(index);
                    if (!removed)
                    {
                        throw new IllegalStateException(String.format("Message with unexpected index '%s' received", index));
                    }

                    if (Integer.valueOf(0).equals(index))
                    {
                        acknowledge(ackMode, session, message);
                    }
                }
                catch (Exception e)
                {
                    exception.set(e);
                }
                finally
                {
                    if (pendingMessages.isEmpty() || exception.get() != null)
                    {
                        completionLatch.countDown();
                    }
                }
            });

            boolean completed = completionLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            assertTrue(String.format("Message listener did not receive all messsages within permitted timeout %d ms", TIMEOUT), completed);
            assertNull("Message listener encountered unexpected exception", exception.get());
        }
        finally
        {
            connection.close();
        }

        verifyLeftOvers(ackMode, queue);
    }

    private void verifyLeftOvers(final int ackMode, final Queue queue) throws JMSException, NamingException
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(ackMode == Session.SESSION_TRANSACTED, ackMode);
            MessageConsumer consumer = session.createConsumer(queue);

            if (ackMode == Session.SESSION_TRANSACTED || ackMode == Session.CLIENT_ACKNOWLEDGE)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull("Second message has not been received on new connection", message);
                assertEquals("Unexpected message index received after restart", 1, message.getIntProperty(INDEX));

                acknowledge(ackMode, session, message);
            }
            else if (ackMode == Session.AUTO_ACKNOWLEDGE)
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNull("Unexpected message received on new connection", message);
            }
            // With Session.DUPS_OK_ACKNOWLEDGE JMS does allow us to be certain if the message will be
            // delivered again or not.
        }
        finally
        {
            connection.close();
        }
    }

    private void acknowledge(final int ackMode, final Session session, final Message message) throws JMSException
    {
        switch(ackMode)
        {
            case Session.SESSION_TRANSACTED:
                session.commit();
                break;
            case Session.CLIENT_ACKNOWLEDGE:
                message.acknowledge();
                break;
            default:
        }
    }
}
