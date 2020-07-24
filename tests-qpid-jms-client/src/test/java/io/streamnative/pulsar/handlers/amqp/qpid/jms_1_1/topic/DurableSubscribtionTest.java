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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.topic;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Arrays;
import java.util.List;
import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * DurableSubscribtionTest.
 */
public class DurableSubscribtionTest extends JmsTestBase
{
    @Test
    public void publishedMessagesAreSavedAfterSubscriberClose() throws Exception
    {
        Topic topic = createTopic(getTestName());
        String subscriptionName = getTestName() + "_sub";
        String clientId = "testClientId";

        TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = producerSession.createProducer(topic);

            Session durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            connection.start();

            producer.send(producerSession.createTextMessage("A"));

            Message message = durableSubscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("A", ((TextMessage) message).getText());

            durableSubscriberSession.commit();

            producer.send(producerSession.createTextMessage("B"));

            message = durableSubscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("B", ((TextMessage) message).getText());

            durableSubscriberSession.rollback();

            durableSubscriber.close();
            durableSubscriberSession.close();

            producer.send(producerSession.createTextMessage("C"));
        }
        finally
        {
            connection.close();
        }

        if (getBrokerAdmin().supportsRestart())
        {
            getBrokerAdmin().restart();
        }

        TopicConnection connection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            connection2.start();
            final Session durableSubscriberSession = connection2.createSession(true, Session.SESSION_TRANSACTED);
            final TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            final List<String> expectedMessages = Arrays.asList("B", "C");
            for (String expectedMessageText : expectedMessages)
            {
                final Message message = durableSubscriber.receive(getReceiveTimeout());
                assertTrue(message instanceof TextMessage);
                assertEquals(expectedMessageText, ((TextMessage) message).getText());

                durableSubscriberSession.commit();
            }

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);
        }
        finally
        {
            connection2.close();
        }
    }

    @Test
    public void testUnsubscribe() throws Exception
    {
        Topic topic = createTopic(getTestName());
        String subscriptionName = getTestName() + "_sub";
        String clientId = "clientId";
        int numberOfQueuesBeforeTest = getQueueCount();

        Connection connection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            Session durableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            Session nonDurableSubscriberSession = connection.createSession(true, Session.SESSION_TRANSACTED);
            Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer subscriber = nonDurableSubscriberSession.createConsumer(topic);
            MessageProducer producer = producerSession.createProducer(topic);
            TopicSubscriber durableSubscriber =
                    durableSubscriberSession.createDurableSubscriber(topic, subscriptionName);

            connection.start();
            producer.send(nonDurableSubscriberSession.createTextMessage("A"));

            Message message = subscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("A", ((TextMessage) message).getText());

            message = durableSubscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("A", ((TextMessage) message).getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession.commit();

            durableSubscriber.close();
            durableSubscriberSession.unsubscribe(subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("B"));

            Session durableSubscriberSession2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber2 =
                    durableSubscriberSession2.createDurableSubscriber(topic, subscriptionName);

            producer.send(nonDurableSubscriberSession.createTextMessage("C"));

            message = subscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("B", ((TextMessage) message).getText());

            message = subscriber.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("C", ((TextMessage) message).getText());

            message = durableSubscriber2.receive(getReceiveTimeout());
            assertTrue(message instanceof TextMessage);
            assertEquals("C", ((TextMessage) message).getText());

            nonDurableSubscriberSession.commit();
            durableSubscriberSession2.commit();

            assertEquals("Message count should be 0", 0, getTotalDepthOfQueuesMessages());

            durableSubscriber2.close();
            durableSubscriberSession2.unsubscribe(subscriptionName);
        }
        finally
        {
            connection.close();
        }

        int numberOfQueuesAfterTest = getQueueCount();
        assertEquals("Unexpected number of queues", numberOfQueuesBeforeTest, numberOfQueuesAfterTest);
    }

    @Test
    public void unsubscribeTwice() throws Exception
    {
        Topic topic = createTopic(getTestName());
        Connection connection = getConnection();
        String subscriptionName = getTestName() + "_sub";
        try
        {

            Session subscriberSession = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = subscriberSession.createDurableSubscriber(topic, subscriptionName);
            MessageProducer publisher = subscriberSession.createProducer(topic);

            connection.start();

            publisher.send(subscriberSession.createTextMessage("Test"));
            subscriberSession.commit();

            Message message =  subscriber.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", message instanceof TextMessage);
            assertEquals("Unexpected message", "Test", ((TextMessage)message).getText());
            subscriberSession.commit();
            subscriber.close();
            subscriberSession.unsubscribe(subscriptionName);

            try
            {
                subscriberSession.unsubscribe(subscriptionName);
                fail("expected InvalidDestinationException when unsubscribing from unknown subscription");
            }
            catch (InvalidDestinationException e)
            {
                // PASS
            }
            catch (Exception e)
            {
                fail("expected InvalidDestinationException when unsubscribing from unknown subscription, got: " + e);
            }
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * <ul>
     * <li>create and register a durable subscriber with no message selector
     * <li>try to create another durable with the same name, should fail
     * </ul>
     * <p>
     * QPID-2418
     */
    @Test
    public void multipleSubscribersWithTheSameName() throws Exception
    {
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(subscriptionName);
        Connection conn = getConnection();
        try
        {
            conn.start();
            Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // create and register a durable subscriber with no message selector
            session.createDurableSubscriber(topic, subscriptionName, null, false);

            // try to recreate the durable subscriber
            try
            {
                session.createDurableSubscriber(topic, subscriptionName, null, false);
                fail("Subscription should not have been created");
            }
            catch (JMSException e)
            {
                // pass
            }
        }
        finally
        {
            conn.close();
        }
    }

    @Test
    public void testDurableSubscribeWithTemporaryTopic() throws Exception
    {
        assumeThat("Not investigated - fails on AMQP 1.0", getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session ssn = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = ssn.createTemporaryTopic();
            try
            {
                ssn.createDurableSubscriber(topic, "test");
                fail("expected InvalidDestinationException");
            }
            catch (InvalidDestinationException ex)
            {
                // this is expected
            }
            try
            {
                ssn.createDurableSubscriber(topic, "test", null, false);
                fail("expected InvalidDestinationException");
            }
            catch (InvalidDestinationException ex)
            {
                // this is expected
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void noLocalMessagesNotDelivered() throws Exception
    {
        String noLocalSubscriptionName = getTestName() + "_no_local_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        Connection publishingConnection = getConnectionBuilder().setClientId("publishingConnection").build();
        try
        {
            Session session = publishingConnection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer sessionProducer = session.createProducer(topic);

            Connection noLocalConnection = getConnectionBuilder().setClientId(clientId).build();
            try
            {
                Session noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                MessageProducer noLocalSessionProducer = noLocalSession.createProducer(topic);

                TopicSubscriber noLocalSubscriber =
                        noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                noLocalConnection.start();
                publishingConnection.start();

                noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                noLocalSession.commit();
                sessionProducer.send(session.createTextMessage("Message2"));
                session.commit();

                Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                assertTrue(durableSubscriberMessage instanceof TextMessage);
                assertEquals("Unexpected local message received",
                             "Message2",
                             ((TextMessage) durableSubscriberMessage).getText());
                noLocalSession.commit();
            }
            finally
            {
                noLocalConnection.close();
            }

            Connection noLocalConnection2 = getConnectionBuilder().setClientId(clientId).build();
            try
            {
                Session noLocalSession = noLocalConnection2.createSession(true, Session.SESSION_TRANSACTED);
                noLocalConnection2.start();
                TopicSubscriber noLocalSubscriber =
                        noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                try
                {
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    final Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                    assertTrue(durableSubscriberMessage instanceof TextMessage);
                    assertEquals("Unexpected local message received",
                                 "Message3",
                                 ((TextMessage) durableSubscriberMessage).getText());
                    noLocalSession.commit();
                }
                finally
                {
                    noLocalSubscriber.close();
                    noLocalSession.unsubscribe(noLocalSubscriptionName);
                }
            }
            finally
            {
                noLocalConnection2.close();
            }
        }
        finally
        {
            publishingConnection.close();
        }
    }

    /**
     * Tests that messages are delivered normally to a subscriber on a separate connection despite
     * the use of durable subscriber with no-local on the first connection.
     */
    @Test
    public void testNoLocalSubscriberAndSubscriberOnSeparateConnection() throws Exception
    {
        String noLocalSubscriptionName = getTestName() + "_no_local_sub";
        String subscriobtionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        final String clientId = "clientId";

        Connection noLocalConnection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            Connection connection = getConnection();
            try
            {
                Session noLocalSession = noLocalConnection.createSession(true, Session.SESSION_TRANSACTED);
                Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

                MessageProducer noLocalSessionProducer = noLocalSession.createProducer(topic);
                MessageProducer sessionProducer = session.createProducer(topic);

                try
                {
                    TopicSubscriber noLocalSubscriber =
                            noLocalSession.createDurableSubscriber(topic, noLocalSubscriptionName, null, true);
                    TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriobtionName, null, false);
                    noLocalConnection.start();
                    connection.start();

                    noLocalSessionProducer.send(noLocalSession.createTextMessage("Message1"));
                    noLocalSession.commit();
                    sessionProducer.send(session.createTextMessage("Message2"));
                    sessionProducer.send(session.createTextMessage("Message3"));
                    session.commit();

                    Message durableSubscriberMessage = noLocalSubscriber.receive(getReceiveTimeout());
                    assertTrue(durableSubscriberMessage instanceof TextMessage);
                    assertEquals("Unexpected local message received",
                                 "Message2",
                                 ((TextMessage) durableSubscriberMessage).getText());
                    noLocalSession.commit();

                    Message nonDurableSubscriberMessage = subscriber.receive(getReceiveTimeout());
                    assertTrue(nonDurableSubscriberMessage instanceof TextMessage);
                    assertEquals("Unexpected message received",
                                 "Message1",
                                 ((TextMessage) nonDurableSubscriberMessage).getText());

                    session.commit();
                    noLocalSubscriber.close();
                    subscriber.close();
                }
                finally
                {
                    noLocalSession.unsubscribe(noLocalSubscriptionName);
                    session.unsubscribe(subscriobtionName);
                }
            }
            finally
            {
                connection.close();
            }
        }
        finally
        {
            noLocalConnection.close();
        }
    }


    @Test
    public void testResubscribeWithChangedNoLocal() throws Exception
    {
        assumeThat("QPID-8068", getProtocol(), is(equalTo(Protocol.AMQP_1_0)));
        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";
        Connection connection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber durableSubscriber =
                    session.createDurableSubscriber(topic, subscriptionName, null, false);

            MessageProducer producer = session.createProducer(topic);
            producer.send(session.createTextMessage("A"));
            producer.send(session.createTextMessage("B"));
            session.commit();

            connection.start();

            Message receivedMessage = durableSubscriber.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message received", "A", ((TextMessage)receivedMessage).getText());

            session.commit();
        }
        finally
        {
            connection.close();
        }

        connection = getConnectionBuilder().setClientId(clientId).build();
        try
        {
            connection.start();

            Session session2 = connection.createSession(true, Session.SESSION_TRANSACTED);
            TopicSubscriber noLocalSubscriber2 = session2.createDurableSubscriber(topic, subscriptionName, null, true);

            Connection secondConnection = getConnectionBuilder().setClientId("secondConnection").build();
            try
            {
                Session secondSession = secondConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageProducer secondProducer = secondSession.createProducer(topic);
                secondProducer.send(secondSession.createTextMessage("C"));
            }
            finally
            {
                secondConnection.close();
            }

            Message noLocalSubscriberMessage = noLocalSubscriber2.receive(getReceiveTimeout());
            assertTrue("TextMessage should be received", noLocalSubscriberMessage instanceof TextMessage);
            assertEquals("Unexpected message received", "C", ((TextMessage)noLocalSubscriberMessage).getText());
        }
        finally
        {
            connection.close();
        }
    }

    /**
     * create and register a durable subscriber with a message selector and then close it
     * crash the broker
     * create a publisher and send  5 right messages and 5 wrong messages
     * recreate the durable subscriber and check we receive the 5 expected messages
     */
    @Test
    public void testMessageSelectorRecoveredOnBrokerRestart() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        final Topic topic = createTopic(getTestName());

        String clientId = "testClientId";
        String subscriptionName = getTestName() + "_sub";
        TopicConnection subscriberConnection =
                (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection.start();
            subscriber.close();
            session.close();
        }
        finally
        {
            subscriberConnection.close();
        }

        getBrokerAdmin().restart();

        TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 0; i < 10; i++)
            {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }
        finally
        {
            connection.close();
        }

        TopicConnection subscriberConnection2 =
                (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection2.start();
            for (int i = 0; i < 5; i++)
            {
                Message message = subscriber.receive(1000);
                if (message == null)
                {
                    fail(String.format("Message '%d' was received", i));
                }
                else
                {
                    assertTrue(String.format("Received message %d with not matching selector", i),
                               message.getStringProperty("testprop").equals("true"));
                }
            }
            subscriber.close();
            session.unsubscribe(subscriptionName);
        }
        finally
        {
            subscriberConnection2.close();
        }
    }

    /**
     * create and register a durable subscriber without a message selector and then unsubscribe it
     * create and register a durable subscriber with a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber with a message selector
     * verify only the matching messages are received
     */
    @Test
    public void testChangeSubscriberToHaveSelector() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        final String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String testClientId = "testClientId";

        TopicConnection subscriberConnection =
                (TopicConnection) getConnectionBuilder().setClientId(testClientId).build();
        try
        {
            TopicSession session = subscriberConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);

            TopicPublisher publisher = session.createPublisher(topic);
            publisher.send(session.createTextMessage("Message1"));
            publisher.send(session.createTextMessage("Message2"));

            subscriberConnection.start();
            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message1", ((TextMessage) receivedMessage).getText());

            subscriber.close();
            session.close();
        }
        finally
        {
            subscriberConnection.close();
        }

        //create and register a durable subscriber with a message selector and then close it
        TopicConnection subscriberConnection2 =
                (TopicConnection) getConnectionBuilder().setClientId(testClientId).build();
        try
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);

            TopicPublisher publisher = session.createPublisher(topic);
            TextMessage message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message4");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            subscriberConnection2.start();

            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message4", ((TextMessage) receivedMessage).getText());

            subscriber.close();
            session.close();
        }
        finally
        {
            subscriberConnection2.close();
        }

        getBrokerAdmin().restart();

        TopicConnection publisherConnection = getTopicConnection();
        try
        {
            TopicSession session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 0; i < 10; i++)
            {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }
        finally
        {
            publisherConnection.close();
        }

        TopicConnection subscriberConnection3 =
                (TopicConnection) getConnectionBuilder().setClientId(testClientId).build();
        try
        {
            TopicSession session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);
            subscriberConnection3.start();

            for (int i = 0; i < 5; i++)
            {
                Message message = subscriber.receive(2000);
                if (message == null)
                {
                    fail(String.format("Message '%d'  was not received", i));
                }
                else
                {
                    assertTrue(String.format("Received message %d with not matching selector", i),
                               message.getStringProperty("testprop").equals("true"));
                }
            }

            subscriber.close();
            session.unsubscribe(subscriptionName);
            session.close();
        }
        finally
        {
            subscriberConnection3.close();
        }
    }


    /**
     * create and register a durable subscriber with a message selector and then unsubscribe it
     * create and register a durable subscriber without a message selector and then close it
     * restart the broker
     * send matching and non matching messages
     * recreate and register the durable subscriber without a message selector
     * verify ALL the sent messages are received
     */
    @Test
    public void testChangeSubscriberToHaveNoSelector() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        final String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        //create and register a durable subscriber with selector then unsubscribe it
        TopicConnection durConnection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = durConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber =
                    session.createDurableSubscriber(topic, subscriptionName, "testprop='true'", false);

            TopicPublisher publisher = session.createPublisher(topic);
            TextMessage message = session.createTextMessage("Messag1");
            message.setStringProperty("testprop", "false");
            publisher.send(message);
            message = session.createTextMessage("Message2");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            message = session.createTextMessage("Message3");
            message.setStringProperty("testprop", "true");
            publisher.send(message);

            durConnection.start();

            Message receivedMessage = subscriber.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message2", ((TextMessage) receivedMessage).getText());

            subscriber.close();
            session.close();
        }
        finally
        {
            durConnection.close();
        }

        //create and register a durable subscriber without the message selector and then close it
        TopicConnection subscriberConnection2 =
                (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = subscriberConnection2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);
            subscriberConnection2.start();
            subscriber.close();
            session.close();
        }
        finally
        {
            subscriberConnection2.close();
        }

        //send messages matching and not matching the original used selector
        TopicConnection publisherConnection = getTopicConnection();
        try
        {
            TopicSession session = publisherConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            for (int i = 1; i <= 10; i++)
            {
                Message message = session.createMessage();
                message.setStringProperty("testprop", String.valueOf(i % 2 == 0));
                publisher.publish(message);
            }
            publisher.close();
            session.close();
        }
        finally
        {
            publisherConnection.close();
        }

        getBrokerAdmin().restart();

        TopicConnection subscriberConnection3 =
                (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            TopicSession session = (TopicSession) subscriberConnection3.createSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicSubscriber subscriber = session.createDurableSubscriber(topic, subscriptionName);
            subscriberConnection3.start();

            for (int i = 1; i <= 10; i++)
            {
                Message message = subscriber.receive(2000);
                if (message == null)
                {
                    fail(String.format("Message %d  was not received", i));
                }
            }

            subscriber.close();
            session.unsubscribe(subscriptionName);
            session.close();
        }
        finally
        {
            subscriberConnection3.close();
        }
    }

    @Test
    public void testResubscribeWithChangedSelector() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(true));

        String subscriptionName = getTestName() + "_sub";
        Topic topic = createTopic(getTestName());
        String clientId = "testClientId";

        TopicConnection connection = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(topic);

            // Create durable subscriber that matches A
            TopicSubscriber subscriberA =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = True", false);

            // Send 1 non-matching message and 1 matching message
            TextMessage message = session.createTextMessage("Message1");
            message.setBooleanProperty("Match", false);
            producer.send(message);
            message = session.createTextMessage("Message2");
            message.setBooleanProperty("Match", true);
            producer.send(message);

            Message receivedMessage = subscriberA.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message2", ((TextMessage) receivedMessage).getText());

            // Send another 1 matching message and 1 non-matching message
            message = session.createTextMessage("Message3");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message4");
            message.setBooleanProperty("Match", false);
            producer.send(message);

            // Disconnect subscriber without receiving the message to
            //leave it on the underlying queue
            subscriberA.close();

            // Reconnect with new selector that matches B
            TopicSubscriber subscriberB = session.createDurableSubscriber(topic,
                                                                          subscriptionName,
                                                                          "Match = False", false);

            // Check that new messages are received properly
            message = session.createTextMessage("Message5");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message6");
            message.setBooleanProperty("Match", false);
            producer.send(message);

            // changing the selector should have cleared the queue so we expect message 6 instead of message 4
            receivedMessage = subscriberB.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message6", ((TextMessage) receivedMessage).getText());

            // publish a message to be consumed after restart
            message = session.createTextMessage("Message7");
            message.setBooleanProperty("Match", true);
            producer.send(message);
            message = session.createTextMessage("Message8");
            message.setBooleanProperty("Match", false);
            producer.send(message);
            session.close();
        }
        finally
        {
            connection.close();
        }

        //now restart the server
        getBrokerAdmin().restart();

        // Reconnect to broker
        TopicConnection connection2 = (TopicConnection) getConnectionBuilder().setClientId(clientId).build();
        try
        {
            connection2.start();
            Session session = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Reconnect with new selector that matches B
            TopicSubscriber subscriberC =
                    session.createDurableSubscriber(topic, subscriptionName, "Match = False", false);

            //check the dur sub's underlying queue now has msg count 1
            Message receivedMessage = subscriberC.receive(getReceiveTimeout());
            assertTrue(receivedMessage instanceof TextMessage);
            assertEquals("Unexpected message content", "Message8", ((TextMessage) receivedMessage).getText());

            subscriberC.close();
            session.unsubscribe(subscriptionName);

            session.close();
        }
        finally
        {
            connection2.close();
        }
    }
}
