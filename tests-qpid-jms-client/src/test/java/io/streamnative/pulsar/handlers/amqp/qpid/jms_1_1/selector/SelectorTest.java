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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.selector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.InvalidSelectorException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.junit.Test;

/**
 * SelectorTest.
 */
public class SelectorTest extends JmsTestBase
{
    private static final String INVALID_SELECTOR = "Cost LIKE 5";

    @Test
    public void invalidSelector() throws Exception
    {
        Connection connection = getConnection();
        Queue queue = createQueue(getTestName());
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                session.createConsumer(queue, INVALID_SELECTOR);
                fail("Exception not thrown");
            }
            catch (InvalidSelectorException e)
            {
                // PASS
            }

            try
            {
                session.createBrowser(queue, INVALID_SELECTOR);
                fail("Exception not thrown");
            }
            catch (InvalidSelectorException e)
            {
                // PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void runtimeSelectorError() throws Exception
    {
        Connection connection = getConnection();
        Queue queue = createQueue(getTestName());
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue , "testproperty % 5 = 1");
            MessageProducer producer = session.createProducer(queue);

            Message message = session.createMessage();
            message.setIntProperty("testproperty", 1); // 1 % 5
            producer.send(message);

            connection.start();

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertNotNull("Message matching selector should be received", receivedMessage);

            message.setStringProperty("testproperty", "hello"); // "hello" % 5 would cause a runtime error
            producer.send(message);
            receivedMessage = consumer.receive(getReceiveTimeout());
            assertNull("Message causing runtime selector error should not be received", receivedMessage);

            MessageConsumer consumerWithoutSelector = session.createConsumer(queue);
            receivedMessage = consumerWithoutSelector.receive(getReceiveTimeout());
            assertNotNull("Message that previously caused a runtime error should be consumable by another consumer", receivedMessage);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void selectorWithJMSMessageID() throws Exception
    {
        Connection connection = getConnection();
        Queue queue = createQueue(getTestName());

        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message firstMessage = session.createMessage();
            Message secondMessage = session.createMessage();
            producer.send(firstMessage);
            producer.send(secondMessage);

            assertNotNull(firstMessage.getJMSMessageID());
            assertNotNull(secondMessage.getJMSMessageID());
            assertNotEquals(firstMessage.getJMSMessageID(), secondMessage.getJMSMessageID());

            MessageConsumer consumer = session.createConsumer(queue, String.format("JMSMessageID = '%s'", secondMessage.getJMSMessageID()));

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertEquals("Unexpected message received", secondMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void selectorWithJMSDeliveryMode() throws Exception
    {
        Connection connection = getConnection();
        Queue queue = createQueue(getTestName());

        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
            Message firstMessage = session.createMessage();
            Message secondMessage = session.createMessage();
            producer.send(firstMessage, DeliveryMode.NON_PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);
            producer.send(secondMessage, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY, Message.DEFAULT_TIME_TO_LIVE);

            MessageConsumer consumer = session.createConsumer(queue, "JMSDeliveryMode = 'PERSISTENT'");

            Message receivedMessage = consumer.receive(getReceiveTimeout());
            assertEquals("Unexpected message received", secondMessage.getJMSMessageID(), receivedMessage.getJMSMessageID());
        }
        finally
        {
            connection.close();
        }
    }
}
