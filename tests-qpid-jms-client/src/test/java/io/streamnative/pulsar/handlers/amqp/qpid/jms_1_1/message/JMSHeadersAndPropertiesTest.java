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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.message;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Enumeration;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.junit.Ignore;
import org.junit.Test;

/**
 * JMSHeadersAndPropertiesTest.
 */
public class JMSHeadersAndPropertiesTest extends JmsTestBase
{

    @Test
    public void resentJMSMessageGetsReplacementJMSMessageID() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            final Message sentMessage = session.createMessage();
            producer.send(sentMessage);

            final String originalId = sentMessage.getJMSMessageID();
            assertNotNull("JMSMessageID must be set after first publish", originalId);

            producer.send(sentMessage);
            final String firstResendID = sentMessage.getJMSMessageID();
            assertNotNull("JMSMessageID must be set after first resend", firstResendID);
            assertNotSame("JMSMessageID must be changed second publish", originalId, firstResendID);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void redelivered() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(1).build();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertFalse("Unexpected JMSRedelivered after first receive", message.getJMSRedelivered());
            assertEquals("Unexpected message content", "A", ((TextMessage) message).getText());

            session.rollback();

            message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertTrue("Unexpected JMSRedelivered after second receive", message.getJMSRedelivered());
            assertEquals("Unexpected message content", "A", ((TextMessage) message).getText());

            message = consumer.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertFalse("Unexpected JMSRedelivered for second message", message.getJMSRedelivered());
            assertEquals("Unexpected message content", "B", ((TextMessage) message).getText());

            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void headers() throws Exception
    {
        final Queue queue = createQueue(getTestName());
        final Destination replyTo = createQueue(getTestName() + "_replyTo");
        final Connection consumerConnection = getConnection();
        try
        {
            final Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = consumerSession.createConsumer(queue);

            final String correlationId = "testCorrelationId";
            final String jmsType = "testJmsType";

            final int priority = 1;
            final long timeToLive = 30 * 60 * 1000;
            final Connection producerConnection = getConnection();
            try
            {
                final Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                final MessageProducer producer = producerSession.createProducer(queue);

                final Message message = producerSession.createMessage();
                message.setJMSCorrelationID(correlationId);
                message.setJMSType(jmsType);
                message.setJMSReplyTo(replyTo);

                long currentTime = System.currentTimeMillis();
                producer.send(message, DeliveryMode.NON_PERSISTENT, priority, timeToLive);

                consumerConnection.start();

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertNotNull(receivedMessage);

                assertEquals("JMSCorrelationID mismatch", correlationId, receivedMessage.getJMSCorrelationID());
                assertEquals("JMSType mismatch", message.getJMSType(), receivedMessage.getJMSType());
                assertEquals("JMSReply To mismatch", message.getJMSReplyTo(), receivedMessage.getJMSReplyTo());
                assertTrue("JMSMessageID does not start 'ID:'", receivedMessage.getJMSMessageID().startsWith("ID:"));
                assertEquals("JMSPriority mismatch", priority, receivedMessage.getJMSPriority());
                assertTrue(String.format(
                        "Unexpected JMSExpiration: got '%d', but expected value equals or greater than '%d'",
                        receivedMessage.getJMSExpiration(),
                        currentTime + timeToLive),

                           receivedMessage.getJMSExpiration() >= currentTime + timeToLive
                           && receivedMessage.getJMSExpiration() <= System.currentTimeMillis() + timeToLive);
            }
            finally
            {
                producerConnection.close();
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    @Test
    public void groupIDAndGroupSeq() throws Exception
    {
        final Connection connection = getConnection();
        try
        {
            assumeThat(isJMSXPropertySupported(connection, "JMSXGroupID"), is(equalTo(true)));
            assumeThat(isJMSXPropertySupported(connection, "JMSXGroupSeq"), is(equalTo(true)));

            final String groupId = "testGroup";
            final int groupSequence = 3;
            final Queue queue = createQueue(getTestName());
            final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(queue);
            final MessageConsumer consumer = session.createConsumer(queue);
            final Message message = session.createMessage();
            message.setStringProperty("JMSXGroupID", groupId);
            message.setIntProperty("JMSXGroupSeq", groupSequence);

            producer.send(message);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertEquals("Unexpected JMSXGroupID", groupId, receivedMessage.getStringProperty("JMSXGroupID"));
            assertEquals("Unexpected JMSXGroupSeq", groupSequence, receivedMessage.getIntProperty("JMSXGroupSeq"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void propertyValues() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();

            message.setBooleanProperty("boolean", true);
            message.setByteProperty("byte", Byte.MAX_VALUE);
            message.setShortProperty("short", Short.MAX_VALUE);
            message.setIntProperty("int", Integer.MAX_VALUE);
            message.setFloatProperty("float", Float.MAX_VALUE);
            message.setDoubleProperty("double", Double.MAX_VALUE);

            producer.send(message);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertEquals("Unexpected boolean property value", Boolean.TRUE, message.getBooleanProperty("boolean"));
            assertEquals("Unexpected byte property value", Byte.MAX_VALUE, message.getByteProperty("byte"));
            assertEquals("Unexpected short property value", Short.MAX_VALUE, message.getShortProperty("short"));
            assertEquals("Unexpected int property value", Integer.MAX_VALUE, message.getIntProperty("int"));
            assertEquals("Unexpected float property value", Float.MAX_VALUE, message.getFloatProperty("float"), 0f);
            assertEquals("Unexpected double property value", Double.MAX_VALUE, message.getDoubleProperty("double"), 0d);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void unsupportedObjectPropertyValue() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            try
            {
                message.setObjectProperty("invalidObject", new Exception());
            }
            catch (MessageFormatException e)
            {
                // pass
            }
            String validValue = "validValue";
            message.setObjectProperty("validObject", validValue);
            producer.send(message);

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessage);

            assertFalse("Unexpected property found", message.propertyExists("invalidObject"));
            assertEquals("Unexpected property value", validValue, message.getObjectProperty("validObject"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void disableJMSMessageId() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message message = session.createMessage();
            producer.send(message);
            assertNotNull("Produced message is expected to have a JMSMessageID", message.getJMSMessageID());

            final MessageConsumer consumer = session.createConsumer(queue);
            connection.start();

            final Message receivedMessageWithId = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessageWithId);

            assertNotNull("Received message is expected to have a JMSMessageID", receivedMessageWithId.getJMSMessageID());
            assertEquals("Received message JMSMessageID should match the sent", message.getJMSMessageID(), receivedMessageWithId.getJMSMessageID());

            producer.setDisableMessageID(true);
            producer.send(message);
            assertNull("Produced message is expected to not have a JMSMessageID", message.getJMSMessageID());

            final Message receivedMessageWithoutId = consumer.receive(getReceiveTimeout());
            assertNotNull(receivedMessageWithoutId);
            assertNull("Received message is not expected to have a JMSMessageID", receivedMessageWithoutId.getJMSMessageID());
        }
        finally
        {
            connection.close();
        }
    }


    private boolean isJMSXPropertySupported(final Connection connection, final String propertyName) throws JMSException
    {
        boolean supported = false;
        Enumeration props = connection.getMetaData().getJMSXPropertyNames();
        while (props.hasMoreElements())
        {
            String name = (String) props.nextElement();
            if (name.equals(propertyName))
            {
                supported = true;
            }

        }
        return supported;
    }
}
