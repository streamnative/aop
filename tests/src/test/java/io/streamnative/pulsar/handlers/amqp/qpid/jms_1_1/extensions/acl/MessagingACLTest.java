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
//package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.acl;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.collect.Sets;
//import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
//import org.apache.qpid.server.logging.EventLogger;
//import org.apache.qpid.server.logging.EventLoggerProvider;
//import org.apache.qpid.server.model.ConfiguredObject;
//import org.apache.qpid.server.model.Group;
//import org.apache.qpid.server.model.GroupMember;
//import org.apache.qpid.server.model.Protocol;
//import org.apache.qpid.server.security.group.GroupProviderImpl;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.jms.Connection;
//import javax.jms.JMSException;
//import javax.jms.Message;
//import javax.jms.MessageConsumer;
//import javax.jms.MessageProducer;
//import javax.jms.Queue;
//import javax.jms.Session;
//import javax.jms.TemporaryQueue;
//import javax.jms.TemporaryTopic;
//import javax.jms.TextMessage;
//import javax.naming.NamingException;
//import java.io.StringReader;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//
//import static org.hamcrest.CoreMatchers.equalTo;
//import static org.hamcrest.CoreMatchers.is;
//import static org.hamcrest.CoreMatchers.not;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//import static org.junit.Assert.assertTrue;
//import static org.junit.Assert.fail;
//import static org.junit.Assume.assumeThat;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//
//public class MessagingACLTest extends JmsTestBase
//{
//    private static final Logger LOGGER = LoggerFactory.getLogger(MessagingACLTest.class);
//
//    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
//    private static final String USER1 = "guest";
//    private static final String USER1_PASSWORD = "guest";
//    private static final String USER2 = "admin";
//    private static final String USER2_PASSWORD = "admin";
//    private static final String RULE_BASED_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER_TYPE =
//            "org.apache.qpid.RuleBaseVirtualHostAccessControlProvider";
//    private static final String EXCHANGE_TYPE = "org.apache.qpid.Exchange";
//
//    @Test
//    public void testAccessAuthorizedSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            assertConnection(connection);
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testAccessNoRightsFailure() throws Exception
//    {
//        configureACL(String.format("ACL DENY-LOG %s ACCESS VIRTUALHOST", USER1));
//
//        try
//        {
//            getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//            fail("Connection was created.");
//        }
//        catch (JMSException e)
//        {
//            assertAccessDeniedException(e);
//        }
//    }
//
//    @Test
//    public void testAccessVirtualHostWithName() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST name='%s'",
//                                   USER1,
//                                   getVirtualHostName()),
//                     String.format("ACL DENY-LOG %s ACCESS VIRTUALHOST name='%s'", USER2, getVirtualHostName()));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            assertConnection(connection);
//        }
//        finally
//        {
//
//            connection.close();
//        }
//
//        try
//        {
//            getConnectionBuilder().setUsername(USER2).setPassword(USER2_PASSWORD).build();
//            fail("Access should be denied");
//        }
//        catch (JMSException e)
//        {
//            assertAccessDeniedException(e);
//        }
//    }
//
//    @Test
//    public void testAccessVirtualHostWildCard() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST name='*'", USER1),
//                     String.format("ACL DENY-LOG %s ACCESS VIRTUALHOST name='*'", USER2));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            assertConnection(connection);
//        }
//        finally
//        {
//
//            connection.close();
//        }
//
//        try
//        {
//            getConnectionBuilder().setUsername(USER2).setPassword(USER2_PASSWORD).build();
//            fail("Access should be denied");
//        }
//        catch (JMSException e)
//        {
//            assertAccessDeniedException(e);
//        }
//    }
//
//    @Test
//    public void testAuthorizationWithConnectionLimit() throws Exception
//    {
//        final int connectionLimit = 2;
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST connection_limit='%d'",
//                                   USER2,
//                                   connectionLimit));
//
//        final List<Connection> establishedConnections = new ArrayList<>();
//        try
//        {
//            establishConnections(connectionLimit, establishedConnections);
//
//            verifyConnectionEstablishmentFails(connectionLimit);
//
//            establishedConnections.remove(0).close();
//            getConnectionBuilder().setUsername(USER2).setPassword(USER2_PASSWORD).build().close();
//        }
//        finally
//        {
//            closeConnections(establishedConnections);
//        }
//    }
//
//    @Test
//    public void testAuthorizationWithConnectionFrequencyLimit() throws Exception
//    {
//        final int connectionFrequencyLimit = 1;
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST connection_frequency_limit='%d'",
//                                   USER2,
//                                   connectionFrequencyLimit));
//
//        final List<Connection> establishedConnections = new ArrayList<>();
//        try
//        {
//            establishConnections(connectionFrequencyLimit, establishedConnections);
//
//            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
//
//            establishedConnections.remove(0).close();
//
//            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
//        }
//        finally
//        {
//            closeConnections(establishedConnections);
//        }
//    }
//
//    @Test
//    public void testAuthorizationWithConnectionLimitAndFrequencyLimit() throws Exception
//    {
//        final int connectionFrequencyLimit = 2;
//        final int connectionLimit = 3;
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST connection_limit='%d' connection_frequency_limit='%d'",
//                                   USER2,
//                                   connectionLimit,
//                                   connectionFrequencyLimit));
//
//        final List<Connection> establishedConnections = new ArrayList<>();
//        try
//        {
//            establishConnections(connectionFrequencyLimit, establishedConnections);
//
//            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
//
//            establishedConnections.remove(0).close();
//
//            verifyConnectionEstablishmentFails(connectionFrequencyLimit);
//        }
//        finally
//        {
//            closeConnections(establishedConnections);
//        }
//    }
//
//    private void establishConnections(final int connectionNumber, final List<Connection> establishedConnections)
//            throws NamingException, JMSException
//    {
//        for (int i = 0; i < connectionNumber; i++)
//        {
//            establishedConnections.add(getConnectionBuilder().setUsername(USER2)
//                                                             .setPassword(USER2_PASSWORD)
//                                                             .setClientId(getTestName() + i)
//                                                             .build());
//        }
//    }
//
//    private void closeConnections(final List<Connection> establishedConnections) throws JMSException
//    {
//        for (Connection c : establishedConnections)
//        {
//            c.close();
//        }
//    }
//
//    private void verifyConnectionEstablishmentFails(final int frequencyLimit) throws NamingException
//    {
//        try
//        {
//            final Connection connection = getConnectionBuilder().setUsername(USER2)
//                                                                .setPassword(USER2_PASSWORD)
//                                                                .setClientId(getTestName() + frequencyLimit)
//                                                                .build();
//            try
//            {
//                fail("Connection creation should fail due to exceeding limit");
//            }
//            finally
//            {
//                connection.close();
//            }
//        }
//        catch (JMSException e)
//        {
//            //pass
//        }
//    }
//
//    @Test
//    public void testConsumeFromTempQueueSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"", USER1),
//                     String.format("ACL ALLOW-LOG %s CONSUME QUEUE temporary=\"true\"", USER1),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\"", USER1) : "");
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//            session.createConsumer(session.createTemporaryQueue()).close();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testConsumeFromTempQueueFailure() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"", USER1),
//                     String.format("ACL DENY-LOG %s CONSUME QUEUE temporary=\"true\"", USER1),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\"", USER1) : "");
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//
//            TemporaryQueue temporaryQueue = session.createTemporaryQueue();
//            try
//            {
//                session.createConsumer(temporaryQueue);
//                fail("Exception is not thrown");
//            }
//            catch (JMSException e)
//            {
//                // pass
//            }
//        }
//        finally
//        {
//            try
//            {
//                connection.close();
//            }
//            catch (Exception e)
//            {
//                LOGGER.error("Unexpected exception on connection close", e);
//            }
//        }
//    }
//
//    @Test
//    public void testConsumeOwnQueueSuccess() throws Exception
//    {
//        final String queueName = "user1Queue";
//        assumeThat(getBrokerAdmin().getValidUsername(), is(equalTo(USER1)));
//
//        createQueue(queueName);
//
//        Map<String, Object> queueAttributes = readEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", true);
//        assertThat("Test prerequiste not met, queue belongs to unexpected user", queueAttributes.get(ConfiguredObject.CREATED_BY), is(equalTo(USER1)));
//
//        configureACL("ACL ALLOW-LOG ALL ACCESS VIRTUALHOST",
//                     "ACL ALLOW-LOG OWNER CONSUME QUEUE",
//                     "ACL DENY-LOG ALL CONSUME QUEUE");
//
//        final String queueAddress = String.format(isLegacyClient() ? "ADDR:%s; {create:never}" : "%s", queueName);
//
//        Connection queueOwnerCon = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session queueOwnerSession = queueOwnerCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            final Queue queue = queueOwnerSession.createQueue(queueAddress);
//            queueOwnerSession.createConsumer(queue).close();
//        }
//        finally
//        {
//            queueOwnerCon.close();
//        }
//
//        Connection otherUserCon = getConnectionBuilder().setUsername(USER2).setPassword(USER2_PASSWORD).build();
//        try
//        {
//            Session otherUserSession = otherUserCon.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            try
//            {
//                otherUserSession.createConsumer(otherUserSession.createQueue(queueAddress)).close();
//                fail("Exception not thrown");
//            }
//            catch (JMSException e)
//            {
//                final String expectedMessage =
//                        Sets.newHashSet(Protocol.AMQP_1_0, Protocol.AMQP_0_10).contains(getProtocol())
//                                ? "Permission CREATE is denied for : Consumer"
//                                : "403(access refused)";
//                assertJMSExceptionMessageContains(e, expectedMessage);
//            }
//        }
//        finally
//        {
//            otherUserCon.close();
//        }
//    }
//
//    @Test
//    public void testConsumeFromTempTopicSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"", USER1),
//                     String.format("ACL ALLOW-LOG %s CONSUME QUEUE temporary=\"true\"", USER1),
//                     String.format(isLegacyClient()
//                                           ? "ACL ALLOW-LOG %s BIND EXCHANGE name=\"amq.topic\""
//                                           : "ACL ALLOW-LOG %s BIND EXCHANGE temporary=\"true\"", USER1));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//            TemporaryTopic temporaryTopic = session.createTemporaryTopic();
//            session.createConsumer(temporaryTopic);
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testConsumeFromNamedQueueValid() throws Exception
//    {
//        final String queueName = getTestName();
//        Queue queue = createQueue(queueName);
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CONSUME QUEUE name=\"%s\"", USER1, queueName),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s CREATE QUEUE name=\"%s\"", USER1, queueName) : "",
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\" routingKey=\"%s\"", USER1, queueName) : "");
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//            session.createConsumer(queue).close();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testConsumeFromNamedQueueFailure() throws Exception
//    {
//        String queueName = getTestName();
//        Queue queue = createQueue(queueName);
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL DENY-LOG %s CONSUME QUEUE name=\"%s\"", USER1, queueName),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s CREATE QUEUE name=\"%s\"", USER1, queueName) : "",
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\" routingKey=\"%s\"", USER1, queueName) : "");
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//            try
//            {
//                session.createConsumer(queue);
//                fail("Test failed as consumer was created.");
//            }
//            catch (JMSException e)
//            {
//                // pass
//            }
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testCreateTemporaryQueueSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE temporary=\"true\"", USER1),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\" temporary=true", USER1) : "");
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            TemporaryQueue queue = session.createTemporaryQueue();
//            assertNotNull(queue);
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    // For AMQP 1.0 the server causes a temporary instance of the fanout exchange to come into being.
//    // For early AMQP version, there are no server side objects created as amq.topic is used.
//    @Test
//    public void testCreateTempTopicSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1));
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            TemporaryTopic temporaryTopic = session.createTemporaryTopic();
//            assertNotNull(temporaryTopic);
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testCreateTemporaryQueueFailed() throws Exception
//    {
//        assumeThat("QPID-7919",
//                   getProtocol(),
//                   is(not(equalTo(Protocol.AMQP_1_0))));
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL DENY-LOG %s CREATE QUEUE temporary=\"true\"", USER1));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            connection.start();
//
//            try
//            {
//                session.createTemporaryQueue();
//                fail("Test failed as creation succeeded.");
//            }
//            catch (JMSException e)
//            {
//                assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Queue");
//            }
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testPublishUsingTransactionSuccess() throws Exception
//    {
//        String queueName = getTestName();
//        Queue queue = createQueue(queueName);
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format(isLegacyClient()
//                                           ? "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"%s\""
//                                           : "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"",
//                                   USER1,
//                                   queueName));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageProducer sender = session.createProducer(queue);
//            sender.send(session.createTextMessage("test"));
//            session.commit();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testPublishToExchangeUsingTransactionSuccess() throws Exception
//    {
//        String queueName = getTestName();
//        createQueue(queueName);
//        final Map<String, Object> bindingArguments = new HashMap<>();
//        bindingArguments.put("destination", queueName);
//        bindingArguments.put("bindingKey", queueName);
//
//        performOperationUsingAmqpManagement("amq.direct",
//                                            "bind",
//                                            EXCHANGE_TYPE,
//                                            bindingArguments);
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"%s\"", USER1, queueName));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//
//            String address = String.format(isLegacyClient() ? "ADDR:amq.direct/%s" : "amq.direct/%s", queueName);
//            Queue queue = session.createQueue(address);
//            MessageProducer sender = session.createProducer(queue);
//            sender.send(session.createTextMessage("test"));
//            session.commit();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testRequestResponseSuccess() throws Exception
//    {
//        String queueName = getTestName();
//        Queue queue = createQueue(queueName);
//        String groupName = "messaging-users";
//        createGroupProvider(groupName, USER1, USER2);
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", groupName),
//                     String.format("ACL ALLOW-LOG %s CONSUME QUEUE name=\"%s\"", USER1, queueName),
//                     String.format("ACL ALLOW-LOG %s CONSUME QUEUE temporary=true", USER2),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE temporary=true", USER2),
//                     isLegacyClient() ?
//                             String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"amq.direct\" temporary=true", USER2) :
//                             String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"TempQueue*\"", USER1),
//                     isLegacyClient() ?
//                             String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"%s\"", USER2, queueName) :
//                             String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"", USER2, queueName),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s CREATE QUEUE name=\"%s\"", USER1, queueName) : "",
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE", USER1) : "",
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"TempQueue*\"", USER1) : ""
//                     );
//
//        Connection responderConnection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session responderSession = responderConnection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageConsumer requestConsumer = responderSession.createConsumer(queue);
//            responderConnection.start();
//
//            Connection requesterConnection = getConnectionBuilder().setUsername(USER2).setPassword(USER2_PASSWORD).build();
//            try
//            {
//                Session requesterSession = requesterConnection.createSession(true, Session.SESSION_TRANSACTED);
//                Queue responseQueue = requesterSession.createTemporaryQueue();
//                MessageConsumer responseConsumer = requesterSession.createConsumer(responseQueue);
//                requesterConnection.start();
//
//                Message request = requesterSession.createTextMessage("Request");
//                request.setJMSReplyTo(responseQueue);
//
//                requesterSession.createProducer(queue).send(request);
//                requesterSession.commit();
//
//                Message receivedRequest = requestConsumer.receive(getReceiveTimeout());
//                assertNotNull("Request is not received", receivedRequest);
//                assertNotNull("Request should have Reply-To", receivedRequest.getJMSReplyTo());
//
//                MessageProducer responder = responderSession.createProducer(receivedRequest.getJMSReplyTo());
//                responder.send(responderSession.createTextMessage("Response"));
//                responderSession.commit();
//
//                Message receivedResponse = responseConsumer.receive(getReceiveTimeout());
//                requesterSession.commit();
//                assertNotNull("Response is not received", receivedResponse);
//                assertEquals("Unexpected response is received", "Response", ((TextMessage) receivedResponse).getText());
//            }
//            finally
//            {
//                 requesterConnection.close();
//            }
//        }
//        finally
//        {
//            responderConnection.close();
//        }
//
//    }
//
//    @Test
//    public void testPublishToTempTopicSuccess() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.topic\"", USER1) :
//                             String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE temporary=\"true\"", USER1));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            connection.start();
//
//            TemporaryTopic temporaryTopic = session.createTemporaryTopic();
//            MessageProducer producer = session.createProducer(temporaryTopic);
//            producer.send(session.createMessage());
//            session.commit();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testFirewallAllow() throws Exception
//    {
//        configureACL(String.format("ACL ALLOW %s ACCESS VIRTUALHOST from_network=\"127.0.0.1\"", USER1));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            assertConnection(connection);
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testFirewallDeny() throws Exception
//    {
//        configureACL(String.format("ACL DENY %s ACCESS VIRTUALHOST from_network=\"127.0.0.1\"", USER1));
//
//        try
//        {
//            getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//            fail("We expected the connection to fail");
//        }
//        catch (JMSException e)
//        {
//            // pass
//        }
//    }
//
//    @Test
//    public void testPublishToDefaultExchangeSuccess() throws Exception
//    {
//        assumeThat("Test not applicable for AMQP 1.0",
//                   getProtocol(),
//                   is(not(equalTo(Protocol.AMQP_1_0))));
//
//        String queueName = getTestName();
//        createQueue(queueName);
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"", USER1, queueName));
//
//
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageProducer sender = session.createProducer(session.createQueue(String.format("ADDR: %s", queueName)));
//            sender.send(session.createTextMessage("test"));
//            session.commit();
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testPublishToDefaultExchangeFailure() throws Exception
//    {
//        assumeThat("Test not applicable for AMQP 1.0",
//                   getProtocol(),
//                   is(not(equalTo(Protocol.AMQP_1_0))));
//
//        String queueName = getTestName();
//        createQueue(queueName);
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL DENY-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"", USER1, queueName));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageProducer sender = session.createProducer(session.createQueue(String.format("ADDR: %s", queueName)));
//            sender.send(session.createTextMessage("test"));
//            session.commit();
//            fail("Sending to the anonymousExchange without permission should fail");
//        }
//        catch (JMSException e)
//        {
//            assertJMSExceptionMessageContains(e, "Access denied to publish to default exchange");
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    @Test
//    public void testAnonymousProducerFailsToSendMessageIntoDeniedDestination() throws Exception
//    {
//        final String allowedDestinationName =  "example.RequestQueue";
//        final String deniedDestinationName = "deniedQueue";
//        createQueue(allowedDestinationName);
//        createQueue(deniedDestinationName);
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format(isLegacyClient()
//                                           ? "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"amq.direct\" routingKey=\"%s\""
//                                           : "ACL ALLOW-LOG %s PUBLISH EXCHANGE name=\"\" routingKey=\"%s\"", USER1, allowedDestinationName),
//                     String.format("ACL DENY-LOG %s PUBLISH EXCHANGE name=\"*\" routingKey=\"%s\"", USER1, deniedDestinationName));
//
//        Connection connection = getConnectionBuilder().setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageProducer producer = session.createProducer(null);
//            producer.send(session.createQueue(allowedDestinationName), session.createTextMessage("test1"));
//            session.commit();
//        }
//        finally
//        {
//            connection.close();
//        }
//
//        Connection connection2 = getConnectionBuilder().setSyncPublish(true).setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection2.createSession(true, Session.SESSION_TRANSACTED);
//            try
//            {
//                MessageProducer producer = session.createProducer(null);
//                producer.send(session.createQueue(deniedDestinationName), session.createTextMessage("test2"));
//
//                fail("Sending should fail");
//            }
//            catch (JMSException e)
//            {
//                assertJMSExceptionMessageContains(e,
//                                                  String.format(
//                                                          "Permission PERFORM_ACTION(publish) is denied for : %s",
//                                                          (!isLegacyClient() ? "Queue" : "Exchange")));
//            }
//
//            try
//            {
//                session.commit();
//                fail("Commit should fail");
//            }
//            catch (JMSException e)
//            {
//                // pass
//            }
//        }
//        finally
//        {
//            connection2.close();
//        }
//    }
//
//    @Test
//    public void testPublishIntoDeniedDestinationFails() throws Exception
//    {
//        final String deniedDestinationName = "deniedQueue";
//        createQueue(deniedDestinationName);
//
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL DENY-LOG %s PUBLISH EXCHANGE name=\"*\" routingKey=\"%s\"", USER1, deniedDestinationName));
//
//
//        Connection connection = getConnectionBuilder().setSyncPublish(true).setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
//            MessageProducer producer = session.createProducer(session.createQueue(deniedDestinationName));
//            producer.send(session.createTextMessage("test"));
//
//            fail("Sending should fail");
//        }
//        catch (JMSException e)
//        {
//            assertJMSExceptionMessageContains(e,
//                                              String.format(
//                                                      "Permission PERFORM_ACTION(publish) is denied for : %s",
//                                                      (!isLegacyClient() ? "Queue" : "Exchange")));
//        }
//    }
//
//    @Test
//    public void testCreateNamedQueueFailure() throws Exception
//    {
//        assumeThat("Test not applicable for AMQP 1.0",
//                   getProtocol(),
//                   is(not(equalTo(Protocol.AMQP_1_0))));
//
//        String queueName = getTestName();
//        configureACL(String.format("ACL ALLOW-LOG %s ACCESS VIRTUALHOST", USER1),
//                     String.format("ACL ALLOW-LOG %s CREATE QUEUE name=\"%s\"", USER1, queueName),
//                     isLegacyClient() ? String.format("ACL ALLOW-LOG %s BIND EXCHANGE name=\"*\" routingKey=\"%s\"", USER1, queueName) : "");
//
//        Connection connection = getConnectionBuilder().setSyncPublish(true).setUsername(USER1).setPassword(USER1_PASSWORD).build();
//        try
//        {
//            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
//            try
//            {
//                session.createConsumer(session.createQueue("IllegalQueue"));
//                fail("Test failed as Queue creation succeeded.");
//            }
//            catch (JMSException e)
//            {
//                assertJMSExceptionMessageContains(e, "Permission CREATE is denied for : Queue");
//            }
//        }
//        finally
//        {
//            connection.close();
//        }
//    }
//
//    private void assertJMSExceptionMessageContains(final JMSException e, final String expectedMessage)
//    {
//        Set<Throwable> examined = new HashSet<>();
//        Throwable current = e;
//        do
//        {
//            if (current.getMessage().contains(expectedMessage))
//            {
//                return;
//            }
//            examined.add(current);
//            current = current.getCause();
//        }
//        while (current != null && !examined.contains(current));
//        e.printStackTrace();
//        fail(String.format("Unexpected message. Root exception : %s. Expected root or underlying(s) to contain : %s", e.getMessage(), expectedMessage));
//    }
//
//
//    private void configureACL(String... rules) throws Exception
//    {
//        EventLoggerProvider eventLoggerProvider = mock(EventLoggerProvider.class);
//        EventLogger eventLogger = mock(EventLogger.class);
//        when(eventLoggerProvider.getEventLogger()).thenReturn(eventLogger);
//
//        List<AclRule> aclRules = new ArrayList<>();
//        try(StringReader stringReader = new StringReader(Arrays.stream(rules).collect(Collectors.joining(LINE_SEPARATOR))))
//        {
//            RuleSet ruleSet = AclFileParser.parse(stringReader, eventLoggerProvider);
//            final List<Rule> parsedRules = ruleSet.getAllRules();
//            for(final Rule rule: parsedRules)
//            {
//                aclRules.add(new AclRule(){
//                    @Override
//                    public String getIdentity()
//                    {
//                        return rule.getIdentity();
//                    }
//
//                    @Override
//                    public ObjectType getObjectType()
//                    {
//                        return rule.getAction().getObjectType();
//                    }
//
//                    @Override
//                    public LegacyOperation getOperation()
//                    {
//                        return rule.getAction().getOperation();
//                    }
//
//                    @Override
//                    public Map<ObjectProperties.Property, String> getAttributes()
//                    {
//                        return rule.getAttributes();
//                    }
//
//                    @Override
//                    public RuleOutcome getOutcome()
//                    {
//                        return rule.getRuleOutcome();
//                    }
//                });
//            }
//        }
//
//        configureACL(aclRules.toArray(new AclRule[aclRules.size()]));
//    }
//
//    private void configureACL(AclRule... rules) throws Exception
//    {
//        final String serializedRules = new ObjectMapper().writeValueAsString(rules);
//        final Map<String, Object> attributes = new HashMap<>();
//        attributes.put(RuleBasedVirtualHostAccessControlProvider.RULES, serializedRules);
//        attributes.put(RuleBasedVirtualHostAccessControlProvider.DEFAULT_RESULT, "DENIED");
//        createEntityUsingAmqpManagement("acl", RULE_BASED_VIRTUAL_HOST_ACCESS_CONTROL_PROVIDER_TYPE, attributes);
//    }
//
//    private void createGroupProvider(final String groupName, final String... groupMembers) throws Exception
//    {
//        String groupProviderName = "groups";
//        Connection connection = getConnectionBuilder().setVirtualHost("$management").build();
//        try
//        {
//            connection.start();
//            createEntity(groupProviderName,
//                         GroupProviderImpl.class.getName(),
//                         Collections.emptyMap(),
//                         connection);
//
//            createEntity(groupName,
//                         Group.class.getName(),
//                         Collections.singletonMap("object-path", groupProviderName),
//                         connection);
//
//            for (String groupMember: groupMembers)
//            {
//                createEntity(groupMember,
//                             GroupMember.class.getName(),
//                             Collections.singletonMap("object-path", groupProviderName + "/" + groupName),
//                             connection);
//            }
//        }
//        finally
//        {
//            connection.close();
//        }
//
//    }
//
//    private void assertConnection(final Connection connection) throws JMSException
//    {
//        assertNotNull("create session should be successful",
//                      connection.createSession(false, Session.AUTO_ACKNOWLEDGE));
//    }
//
//    private void assertAccessDeniedException(JMSException e) throws Exception
//    {
//        assertTrue("Unexpected exception message:" + e.getMessage(),
//                   e.getMessage().contains("Permission PERFORM_ACTION(connect) is denied"));
//        if (getProtocol() == Protocol.AMQP_1_0)
//        {
//            assertTrue("Unexpected error condition reported:" + e.getMessage(),
//                       e.getMessage().contains("amqp:not-allowed"));
//        }
//    }
//
//    private boolean isLegacyClient()
//    {
//        return getProtocol() != Protocol.AMQP_1_0;
//    }
//}
