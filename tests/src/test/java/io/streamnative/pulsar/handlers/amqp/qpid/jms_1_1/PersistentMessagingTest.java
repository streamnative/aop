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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1;

import static javax.jms.DeliveryMode.PERSISTENT;
import static javax.jms.Session.CLIENT_ACKNOWLEDGE;
import static javax.jms.Session.SESSION_TRANSACTED;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.junit.Before;
import org.junit.Test;

/**
 * PersistentMessagingTest.
 */
public class PersistentMessagingTest extends JmsTestBase
{
    private static final int MSG_COUNT = 3;
    private static final String INT_PROPERTY = "index";
    private static final String STRING_PROPERTY = "string";

    @Before
    public void setUp() throws Exception
    {
        assumeThat("Tests requires persistent store", getBrokerAdmin().supportsRestart(), is(true));
    }

    @Test
    public void committedPersistentMessagesSurviveBrokerRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection sendingConnection = getConnection();
        List<Message> sentMessages = new ArrayList<>();
        try
        {
            Session session = sendingConnection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            sentMessages.addAll(sendMessages(session, producer, PERSISTENT, 0, MSG_COUNT));
//            sendMessages(session, producer, NON_PERSISTENT, MSG_COUNT, 1);
        }
        finally
        {
            sendingConnection.close();
        }

        getBrokerAdmin().restart();

        verifyQueueContents(queue, sentMessages);
    }

    @Test
    public void uncommittedPersistentMessagesDoNotSurviveBrokerRestart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection sendingConnection = getConnection();
        try
        {
            Session session = sendingConnection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            producer.send(session.createMessage());
            // do not commit
        }
        finally
        {
            sendingConnection.close();
        }

        getBrokerAdmin().restart();

        Connection receivingConnection = getConnection();
        try
        {
            receivingConnection.start();
            Session session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);

            final Message unexpectedMessage = consumer.receiveNoWait();
            assertNull(String.format("Unexpected message [%s] received", unexpectedMessage), unexpectedMessage);
        }
        finally
        {
            receivingConnection.close();
        }
    }

    @Test
    public void transactedAcknowledgementPersistence() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection initialConnection = getConnection();
        List<Message> remainingMessages = new ArrayList<>();
        try
        {
            initialConnection.start();
            Session session = initialConnection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            final List<Message> initialMessage = sendMessages(session, producer, PERSISTENT, 0, 1);
            remainingMessages.addAll(sendMessages(session, producer, PERSISTENT, 1, 1));

            // Receive first message and commit
            MessageConsumer consumer = session.createConsumer(queue);
            receiveAndVerifyMessages(session, consumer, initialMessage);
            // Receive second message but do not commit
            final Message peek = consumer.receive(getReceiveTimeout());
            assertNotNull(peek);
        }
        finally
        {
            initialConnection.close();
        }

        getBrokerAdmin().restart();

        verifyQueueContents(queue, remainingMessages);
    }

    @Test
    public void clientAckAcknowledgementPersistence() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection initialConnection = getConnection();
        List<Message> remainingMessages = new ArrayList<>();
        try
        {
            initialConnection.start();
            Session publishingSession = initialConnection.createSession(true, SESSION_TRANSACTED);
            MessageProducer producer = publishingSession.createProducer(queue);

            final List<Message> initialMessages = sendMessages(publishingSession, producer, PERSISTENT, 0, 1);
            remainingMessages.addAll(sendMessages(publishingSession, producer, PERSISTENT, 1, 1));

            Session consumingSession = initialConnection.createSession(false, CLIENT_ACKNOWLEDGE);

            // Receive first message and ack
            MessageConsumer consumer = consumingSession.createConsumer(queue);
            receiveAndVerifyMessages(consumingSession, consumer, initialMessages);

            // Receive second but do not ack
            final Message peek = consumer.receive(getReceiveTimeout());
            assertNotNull(peek);
        }
        finally
        {
            initialConnection.close();
        }

        getBrokerAdmin().restart();

        verifyQueueContents(queue, remainingMessages);
    }

    private List<Message> sendMessages(Session session, MessageProducer producer,
                                       final int deliveryMode,
                                       final int startIndex, final int count) throws Exception
    {
        final List<Message> sentMessages = new ArrayList<>();
        for (int i = startIndex; i < startIndex + count; i++)
        {
            Message message = session.createTextMessage(UUID.randomUUID().toString());
            message.setIntProperty(INT_PROPERTY, i);
            message.setStringProperty(STRING_PROPERTY, UUID.randomUUID().toString());

            producer.send(message, deliveryMode, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            sentMessages.add(message);
        }

        session.commit();
        return sentMessages;
    }

    private void verifyQueueContents(final Queue queue, final List<Message> expectedMessages) throws Exception
    {
        Connection receivingConnection = getConnection();
        try
        {
            receivingConnection.start();
            Session session = receivingConnection.createSession(true, SESSION_TRANSACTED);
            MessageConsumer consumer = session.createConsumer(queue);

            receiveAndVerifyMessages(session, consumer, expectedMessages);

            final Message unexpectedMessage = consumer.receiveNoWait();
            assertNull(String.format("Unexpected additional message [%s] received", unexpectedMessage), unexpectedMessage);
        }
        finally
        {
            receivingConnection.close();
        }
    }

    private void receiveAndVerifyMessages(final Session session,
                                          final MessageConsumer consumer,
                                          final List<Message> expectedMessages) throws Exception
    {

        for (Message expected : expectedMessages)
        {
            final Message received = consumer.receive(getReceiveTimeout());
            assertNotNull(String.format("Message not received when expecting message %d", expected.getIntProperty(INT_PROPERTY)), received);

            assertTrue("Unexpected type", expected instanceof TextMessage);
            assertEquals("Unexpected index",
                         expected.getIntProperty(INT_PROPERTY),
                         received.getIntProperty(INT_PROPERTY));
            assertEquals("Unexpected string property",
                         expected.getStringProperty(STRING_PROPERTY),
                         received.getStringProperty(STRING_PROPERTY));
            assertEquals("Unexpected message content",
                         ((TextMessage) expected).getText(),
                         ((TextMessage) received).getText());

            final int acknowledgeMode = session.getAcknowledgeMode();
            if (acknowledgeMode == SESSION_TRANSACTED)
            {
                session.commit();
            }
            else if (acknowledgeMode == CLIENT_ACKNOWLEDGE)
            {
                received.acknowledge();
            }
        }

    }

}
