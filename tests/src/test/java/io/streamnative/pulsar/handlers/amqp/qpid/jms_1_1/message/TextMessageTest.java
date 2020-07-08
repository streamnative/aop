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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.junit.Test;

/**
 * TextMessageTest.
 */
public class TextMessageTest extends JmsTestBase
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

            TextMessage message = session.createTextMessage(null);
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", receivedMessage instanceof TextMessage);
            assertNull("Unexpected body", ((TextMessage) receivedMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void sendAndReceiveAsciiBody() throws Exception
    {
        sendAndReceiveBody("body");
    }

    @Test
    public void sendAndReceiveNonAsciiUTF8Body() throws Exception
    {
        sendAndReceiveBody("YEN\u00A5EURO\u20AC");
    }

    private void sendAndReceiveBody(final String text) throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer producer = session.createProducer(queue);

            TextMessage message = session.createTextMessage(text);
            producer.send(message);

            MessageConsumer consumer = session.createConsumer(queue);
            connection.start();
            Message receivedMessage = consumer.receive(getReceiveTimeout());

            assertTrue("TextMessage should be received", receivedMessage instanceof TextMessage);

            TextMessage receivedTextMessage = (TextMessage) receivedMessage;

            assertEquals("Unexpected body", receivedTextMessage.getText(), text);
        }
        finally
        {
            connection.close();
        }
    }
}
