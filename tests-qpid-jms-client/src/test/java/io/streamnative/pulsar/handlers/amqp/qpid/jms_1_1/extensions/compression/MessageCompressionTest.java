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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.compression;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.qpid.server.model.Protocol;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * MessageCompressionTest.
 */
@Ignore
public class MessageCompressionTest extends JmsTestBase
{
    private static final int MIN_MESSAGE_PAYLOAD_SIZE = 2048 * 1024;

    @Before
    public void setUp()
    {
        assumeThat("AMQP 1.0 client does not support compression yet", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
    }

    @Test
    public void testSenderCompressesReceiverUncompresses() throws Exception
    {
        doTestCompression(true, true, true);
    }

    @Test
    public void testSenderCompressesOnly() throws Exception
    {
        doTestCompression(true, false, true);
    }

    @Test
    public void testReceiverUncompressesOnly() throws Exception
    {
        doTestCompression(false, true, true);
    }

    @Test
    public void testNoCompression() throws Exception
    {
        doTestCompression(false, false, true);
    }

    @Test
    public void testDisablingCompressionAtBroker() throws Exception
    {
        enableMessageCompression(false);
        try
        {
            doTestCompression(true, true, false);
        }
        finally
        {
            enableMessageCompression(true);
        }
    }


    private void doTestCompression(final boolean senderCompresses,
                                   final boolean receiverUncompresses,
                                   final boolean brokerCompressionEnabled) throws Exception
    {

        String messageText = createMessageText();
        Connection senderConnection = getConnectionBuilder().setCompress(senderCompresses).build();
        String queueName = getTestName();
        Queue testQueue = createQueue(queueName);
        try
        {
            publishMessage(senderConnection, messageText, testQueue);

            Map<String, Object> statistics = getVirtualHostStatistics("bytesIn");
            int bytesIn = ((Number) statistics.get("bytesIn")).intValue();

            if (senderCompresses && brokerCompressionEnabled)
            {
                assertTrue("Message was not sent compressed", bytesIn < messageText.length());
            }
            else
            {
                assertFalse("Message was incorrectly sent compressed", bytesIn < messageText.length());
            }
        }
        finally
        {
            senderConnection.close();
        }

        // receive the message
        Connection consumerConnection = getConnectionBuilder().setCompress(receiverUncompresses).build();
        try
        {
            Session session = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(testQueue);
            consumerConnection.start();

            TextMessage message = (TextMessage) consumer.receive(getReceiveTimeout() * 2);
            assertNotNull("Message was not received", message);
            assertEquals("Message was corrupted", messageText, message.getText());
            assertEquals("Header was corrupted", "foo", message.getStringProperty("bar"));

            Map<String, Object> statistics = getVirtualHostStatistics("bytesOut");
            int bytesOut = ((Number) statistics.get("bytesOut")).intValue();

            if (receiverUncompresses && brokerCompressionEnabled)
            {
                assertTrue("Message was not received compressed", bytesOut < messageText.length());
            }
            else
            {
                assertFalse("Message was incorrectly received compressed", bytesOut < messageText.length());
            }
        }
        finally
        {
            consumerConnection.close();
        }
    }

    private void publishMessage(final Connection senderConnection, final String messageText, final Queue testQueue)
            throws JMSException
    {
        Session session = senderConnection.createSession(true, Session.SESSION_TRANSACTED);

        MessageProducer producer = session.createProducer(testQueue);
        TextMessage sentMessage = session.createTextMessage(messageText);
        sentMessage.setStringProperty("bar", "foo");

        producer.send(sentMessage);
        session.commit();
    }

    private String createMessageText()
    {
        StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < MIN_MESSAGE_PAYLOAD_SIZE)
        {
            stringBuilder.append("This should compress easily. ");
        }
        return stringBuilder.toString();
    }

    private void enableMessageCompression(final boolean value) throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
        try
        {
            connection.start();
            final Map<String, Object> attributes =
                    Collections.singletonMap("messageCompressionEnabled", value);
            updateEntityUsingAmqpManagement("Broker",
                                            "org.apache.qpid.Broker",
                                             attributes,
                                            connection);

        }
        finally
        {
            connection.close();
        }
    }
}
