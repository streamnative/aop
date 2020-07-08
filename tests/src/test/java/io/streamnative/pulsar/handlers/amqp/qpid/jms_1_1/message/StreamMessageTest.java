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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageEOFException;
import javax.jms.MessageNotWriteableException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import org.junit.Test;

/**
 * StreamMessageTest.
 */
public class StreamMessageTest extends JmsTestBase
{
    @Test
    public void testStreamMessageEOF() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection consumerConnection = getConnection();
        try
        {
            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);

            Connection producerConnection = getConnection();
            try
            {
                Session producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = producerSession.createProducer(queue);
                StreamMessage msg = producerSession.createStreamMessage();
                msg.writeByte((byte) 42);
                producer.send(msg);

                consumerConnection.start();

                Message receivedMessage = consumer.receive(getReceiveTimeout());
                assertTrue(receivedMessage instanceof StreamMessage);
                StreamMessage streamMessage = (StreamMessage)receivedMessage;
                streamMessage.readByte();
                try
                {
                    streamMessage.readByte();
                    fail("Expected exception not thrown");
                }
                catch (Exception e)
                {
                    assertTrue("Expected MessageEOFException: " + e, e instanceof MessageEOFException);
                }

                try
                {
                    streamMessage.writeByte((byte) 42);
                    fail("Expected exception not thrown");
                }
                catch (MessageNotWriteableException e)
                {
                    // pass
                }
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
    public void testModifyReceivedMessageContent() throws Exception
    {
        Queue queue = createQueue(getTestName());
        final CountDownLatch awaitMessages = new CountDownLatch(1);
        final AtomicReference<Throwable> listenerCaughtException = new AtomicReference<>();

        Connection consumerConnection = getConnection();
        try
        {
            Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.close();

            Session consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            MessageConsumer consumer = consumerSession.createConsumer(queue);
            consumer.setMessageListener(message -> {
                final StreamMessage sm = (StreamMessage) message;
                try
                {
                    sm.clearBody();
                    // it is legal to extend a stream message's content
                    sm.writeString("dfgjshfslfjshflsjfdlsjfhdsljkfhdsljkfhsd");
                }
                catch (Throwable t)
                {
                    listenerCaughtException.set(t);
                }
                finally
                {
                    awaitMessages.countDown();
                }
            });

            Connection producerConnection = getConnection();
            try
            {
                Session producerSession = producerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
                MessageProducer producer = producerSession.createProducer(queue);

                StreamMessage message = producerSession.createStreamMessage();
                message.writeInt(42);
                producer.send(message);

                consumerConnection.start();
                assertTrue("Message did not arrive with consumer within a reasonable time",
                           awaitMessages.await(getReceiveTimeout(), TimeUnit.SECONDS));
                assertNull("No exception should be caught by listener : " + listenerCaughtException.get(),
                           listenerCaughtException.get());
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
}
