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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import org.junit.Test;

/**
 * JMSReplyToTest.
 */
public class JMSReplyToTest extends JmsTestBase
{
    @Test
    public void testRequestResponseUsingJmsReplyTo() throws Exception
    {
        Queue requestQueue = createQueue(getTestName() + ".request");
        Queue replyToQueue = createQueue(getTestName() + ".reply");
        Connection connection = getConnection();
        try
        {
            AtomicReference<Throwable> exceptionHolder = createAsynchronousConsumer(connection, requestQueue);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer replyConsumer = session.createConsumer(replyToQueue);

            Message requestMessage = session.createTextMessage("Request");
            requestMessage.setJMSReplyTo(replyToQueue);
            MessageProducer producer = session.createProducer(requestQueue);
            producer.send(requestMessage);

            Message responseMessage = replyConsumer.receive(getReceiveTimeout());
            assertNotNull("Response message not received", responseMessage);
            assertEquals("Correlation id of the response should match message id of the request",
                         responseMessage.getJMSCorrelationID(), requestMessage.getJMSMessageID());
            assertNull("Unexpected exception in responder", exceptionHolder.get());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testRequestResponseUsingTemporaryJmsReplyTo() throws Exception
    {
        Queue requestQueue = createQueue(getTestName() + ".request");
        Connection connection = getConnection();
        try
        {
            AtomicReference<Throwable> exceptionHolder = createAsynchronousConsumer(connection, requestQueue);
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TemporaryQueue replyToQueue = session.createTemporaryQueue();

            MessageConsumer replyConsumer = session.createConsumer(replyToQueue);

            Message requestMessage = session.createTextMessage("Request");
            requestMessage.setJMSReplyTo(replyToQueue);
            MessageProducer producer = session.createProducer(requestQueue);
            producer.send(requestMessage);

            Message responseMessage = replyConsumer.receive(getReceiveTimeout());
            assertNotNull("Response message not received", responseMessage);
            assertEquals("Correlation id of the response should match message id of the request",
                         responseMessage.getJMSCorrelationID(), requestMessage.getJMSMessageID());
            assertNull("Unexpected exception in responder", exceptionHolder.get());
        }
        finally
        {
            connection.close();
        }
    }

    private AtomicReference<Throwable> createAsynchronousConsumer(Connection connection, Queue requestQueue)
            throws JMSException
    {
        final Session responderSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final AtomicReference<Throwable> caughtException = new AtomicReference<>();
        final MessageConsumer requestConsumer = responderSession.createConsumer(requestQueue);
        requestConsumer.setMessageListener(message -> {
            try
            {
                Destination replyTo = message.getJMSReplyTo();
                MessageProducer responseProducer = responderSession.createProducer(replyTo);
                Message responseMessage = responderSession.createMessage();
                responseMessage.setJMSCorrelationID(message.getJMSMessageID());
                responseProducer.send(responseMessage);
            }
            catch (Throwable t)
            {
                caughtException.set(t);
            }
        });
        return caughtException;
    }
}
