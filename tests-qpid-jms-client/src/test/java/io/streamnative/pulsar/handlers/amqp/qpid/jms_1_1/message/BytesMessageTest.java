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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.nio.charset.StandardCharsets;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.junit.Test;

/**
 * BytesMessageTest.
 */
public class BytesMessageTest extends JmsTestBase
{
    @Test
    public void sendAndReceiveEmpty() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            BytesMessage message = session.createBytesMessage();
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("BytesMessage should be received", receivedMessage instanceof BytesMessage);
            assertEquals("Unexpected body length", 0, ((BytesMessage) receivedMessage).getBodyLength());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveBody() throws Exception
    {
        final byte[] text = "euler".getBytes(StandardCharsets.US_ASCII);
        final double value = 2.71828;

        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            BytesMessage message = session.createBytesMessage();

            message.writeBytes(text);
            message.writeDouble(value);

            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("BytesMessage should be received", receivedMessage instanceof BytesMessage);

            byte[] receivedBytes = new byte[text.length];
            BytesMessage receivedBytesMessage = (BytesMessage) receivedMessage;
            receivedBytesMessage.readBytes(receivedBytes);

            assertArrayEquals("Unexpected bytes", receivedBytes, text);
            assertEquals("Unexpected double", value, receivedBytesMessage.readDouble(), 0);
        }
        finally
        {
            connection.close();
        }
    }
}
