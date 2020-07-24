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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.producer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.AnyOf.anyOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * AnonymousProducerTest.
 */
public class AnonymousProducerTest extends JmsTestBase
{

    @Test
    public void testPublishIntoDestinationBoundWithNotMatchingFilter() throws Exception
    {
        Topic topic = createTopic(getTestName());
        final Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(null);

            MessageConsumer consumer = session.createConsumer(topic, "id>1");
            TextMessage notMatching = session.createTextMessage("notMatching");
            notMatching.setIntProperty("id", 1);
            messageProducer.send(topic, notMatching);

            TextMessage matching = session.createTextMessage("Matching");
            matching.setIntProperty("id", 2);
            messageProducer.send(topic, matching);
            session.commit();

            connection.start();
            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("Expected message not received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected text", "Matching", textMessage.getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPublishIntoNonExistingTopic() throws Exception
    {
        final Topic topic = createTopic(getTestName());
        final Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(null);
            messageProducer.send(topic, session.createTextMessage("A"));
            session.commit();

            connection.start();
            MessageConsumer consumer = session.createConsumer(topic);
            messageProducer.send(topic, session.createTextMessage("B"));
            session.commit();

            Message message = consumer.receive(getReceiveTimeout());
            assertTrue("Expected message not received", message instanceof TextMessage);
            TextMessage textMessage = (TextMessage) message;
            assertEquals("Unexpected text", "B", textMessage.getText());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testPublishIntoNonExistingQueue() throws Exception
    {
        assumeThat("QPID-7818/QPIDJMS-349", getProtocol(), is(not(anyOf(equalTo(Protocol.AMQP_0_10), equalTo(Protocol.AMQP_1_0)))));
        final Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer messageProducer = session.createProducer(null);
            try
            {
                messageProducer.send(session.createQueue("nonExistingQueue"), session.createTextMessage("testMessage"));
                session.commit();
                fail("Expected exception was not thrown");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testSyncPublishIntoNonExistingQueue() throws Exception
    {
        assumeThat("QPID-7818", getProtocol(), is(not(equalTo(Protocol.AMQP_0_10))));
        final Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(null);
            final Queue queue = session.createQueue("nonExistingQueue");
            try
            {
                producer.send(queue, session.createTextMessage("hello"));
                fail("Send to unknown destination should result in error");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testUnidentifiedDestination() throws Exception
    {
       Connection connection =  getConnection();
       try
       {
           Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
           MessageProducer publisher = session.createProducer(null);
           try
           {
               publisher.send(session.createTextMessage("Test"));
               fail("Did not throw UnsupportedOperationException");
           }
           catch (UnsupportedOperationException e)
           {
               // PASS
           }
       }
       finally
       {
           connection.close();
       }
   }
}
