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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.management;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.qpid.server.model.Queue.ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.AmqpManagementFacade;
import io.streamnative.pulsar.handlers.amqp.qpid.core.BrokerAdmin;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.BrokerManagementHelper;
import io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.TlsHelper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.jms.Connection;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.NamingException;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.Protocol;
import org.apache.qpid.server.model.VirtualHostNode;
import org.apache.qpid.server.queue.PriorityQueue;
import org.apache.qpid.test.utils.tls.TlsResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * AmqpManagementTest.
 */
public class AmqpManagementTest extends JmsTestBase
{
    @ClassRule
    public static final TlsResource TLS_RESOURCE = new TlsResource();

    private static TlsHelper _tlsHelper;

    private Session _session;
    private Queue _replyAddress;
    private MessageConsumer _consumer;
    private MessageProducer _producer;

    @BeforeClass
    public static void setUp() throws Exception
    {
        _tlsHelper = new TlsHelper(TLS_RESOURCE);
    }

    private void setUp(final Connection connection) throws Exception
    {
        connection.start();
        _session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue;
        final Queue replyConsumer;
        if(getProtocol() == Protocol.AMQP_1_0)
        {
            queue = _session.createQueue("$management");
            _replyAddress = _session.createTemporaryQueue();
            replyConsumer = _replyAddress;
        }
        else
        {
            queue = _session.createQueue("ADDR:$management");
            _replyAddress = _session.createQueue("ADDR:!response");
            replyConsumer = _session.createQueue(
                    "ADDR:$management ; {assert : never, node: { type: queue }, link:{name: \"!response\"}}");
        }
        _consumer = _session.createConsumer(replyConsumer);
        _producer = _session.createProducer(queue);
    }

    // test get types on $management
    @Test
    public void testGetTypesOnBrokerManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            Message message = _session.createBytesMessage();

            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "GET-TYPES");

            message.setJMSReplyTo(_replyAddress);

            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertNotNull("The response did not include the org.amqp.Management type",
                          getValueFromMapResponse(responseMessage, "org.amqp.management"));
            assertNotNull("The response did not include the org.apache.qpid.Port type",
                          getValueFromMapResponse(responseMessage, "org.apache.qpid.Port"));
        }
        finally
        {
            connection.close();
        }
    }

    // test get types on $management
    @Test
    public void testQueryBrokerManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);
            MapMessage message = _session.createMapMessage();

            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "QUERY");
            message.setObject("attributeNames", "[]");
            message.setJMSReplyTo(_replyAddress);

            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            assertEquals("The correlation id does not match the sent message's messageId",
                         message.getJMSMessageID(),
                         responseMessage.getJMSCorrelationID());
            checkResponseIsMapType(responseMessage);
            List<String> resultMessageKeys = new ArrayList<>(getMapResponseKeys(responseMessage));
            assertEquals("The response map has two entries", 2, resultMessageKeys.size());
            assertTrue("The response map does not contain attribute names",
                       resultMessageKeys.contains("attributeNames"));
            assertTrue("The response map does not contain results ", resultMessageKeys.contains("results"));
            Object attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
            assertTrue("The attribute names are not a list", attributeNames instanceof Collection);
            Collection attributeNamesCollection = (Collection) attributeNames;
            assertTrue("The attribute names do not contain identity", attributeNamesCollection.contains("identity"));
            assertTrue("The attribute names do not contain name", attributeNamesCollection.contains("name"));

            assertTrue("The attribute names do not contain qpid-type", attributeNamesCollection.contains("qpid-type"));

            // Now test filtering by type
            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "QUERY");
            message.setStringProperty("entityType", "org.apache.qpid.Exchange");

            message.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);

            assertEquals("The correlation id does not match the sent message's messageId",
                         message.getJMSMessageID(),
                         responseMessage.getJMSCorrelationID());
            resultMessageKeys = new ArrayList<>(getMapResponseKeys(responseMessage));
            assertEquals("The response map has two entries", 2, resultMessageKeys.size());
            assertTrue("The response map does not contain attribute names",
                       resultMessageKeys.contains("attributeNames"));
            assertTrue("The response map does not contain results ", resultMessageKeys.contains("results"));
            attributeNames = getValueFromMapResponse(responseMessage, "attributeNames");
            assertTrue("The attribute names are not a list", attributeNames instanceof Collection);
            attributeNamesCollection = (Collection) attributeNames;
            assertEquals("The attributeNames are no as expected",
                         Arrays.asList("name", "identity", "type"),
                         attributeNamesCollection);
            Object resultsObject = getValueFromMapResponse(responseMessage, "results");
            assertTrue("results is not a collection", resultsObject instanceof Collection);
            Collection results = (Collection) resultsObject;

            final int numberOfExchanges = results.size();
            assertTrue("results should have at least 4 elements", numberOfExchanges >= 4);

            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "QUERY");
            message.setStringProperty("entityType", "org.apache.qpid.DirectExchange");

            message.setObject("attributeNames", "[\"name\", \"identity\", \"type\"]");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            final Collection directExchanges = (Collection) getValueFromMapResponse(responseMessage, "results");
            assertTrue(
                    "There are the same number of results when searching for direct exchanges as when searching for all exchanges",
                    directExchanges.size() < numberOfExchanges);
            assertTrue("The list of direct exchanges is not a proper subset of the list of all exchanges",
                       results.containsAll(directExchanges));
        }
        finally
        {
            connection.close();
        }
    }

    // test get types on a virtual host
    @Test
    public void testGetTypesOnVirtualHostManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);
            Message message = _session.createBytesMessage();

            message.setStringProperty("identity", "self");
            message.setStringProperty("type", "org.amqp.management");
            message.setStringProperty("operation", "GET-TYPES");
            String correlationID = "some correlation id";
            message.setJMSCorrelationIDAsBytes(correlationID.getBytes(UTF_8));

            message.setJMSReplyTo(_replyAddress);

            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertNotNull("A response message was not sent", responseMessage);
            assertEquals("The correlation id does not match the sent message's correlationId",
                       correlationID, responseMessage.getJMSCorrelationID());

            assertResponseCode(responseMessage, 200);
            assertNotNull("The response did not include the org.amqp.Management type",
                          getValueFromMapResponse(responseMessage,"org.amqp.management"));
            assertNull("The response included the org.apache.qpid.Port type",
                       getValueFromMapResponse(responseMessage,"org.apache.qpid.Port"));
        }
        finally
        {
            connection.close();
        }

    }

    // create / update / read / delete a queue via $management
    @Test
    public void testCreateQueueOnBrokerManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 100L);
            String path = getVirtualHostName() + "/" + getVirtualHostName() + "/" + getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 201);
            checkResponseIsMapType(responseMessage);
            assertEquals("The created queue was not a standard queue",
                         "org.apache.qpid.StandardQueue",
                         getValueFromMapResponse(responseMessage, "type"));
            assertEquals("The created queue was not a standard queue",
                         "standard",
                         getValueFromMapResponse(responseMessage, "qpid-type"));
            assertEquals("the created queue did not have the correct alerting threshold",
                         100L,
                         getValueFromMapResponse(responseMessage, ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES));
            Object identity = getValueFromMapResponse(responseMessage, "identity");

            message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "UPDATE");
            message.setObjectProperty("identity", identity);
            message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertEquals("the created queue did not have the correct alerting threshold",
                         250L,
                         getValueFromMapResponse(responseMessage, ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES));

            message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "DELETE");
            message.setObjectProperty("index", "object-path");
            message.setObjectProperty("key", path);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 204);

            message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "READ");
            message.setObjectProperty("identity", identity);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 404);
        }
        finally
        {
            connection.close();
        }
    }

    // create / update / read / delete a queue via vhost
    @Test
    public void testCreateQueueOnVirtualHostManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            message.setInt(PriorityQueue.PRIORITIES, 13);
            String path = getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 201);
            checkResponseIsMapType(responseMessage);
            assertEquals("The created queue was not a priority queue",
                         "org.apache.qpid.PriorityQueue",
                         getValueFromMapResponse(responseMessage, "type"));
            assertEquals("The created queue was not a standard queue",
                         "priority",
                         getValueFromMapResponse(responseMessage, "qpid-type"));
            assertEquals("the created queue did not have the correct number of priorities",
                         13,
                         Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString())
                                .intValue());
            Object identity = getValueFromMapResponse(responseMessage, "identity");

            // Trying to create a second queue with the same name should cause a conflict
            message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            message.setInt(PriorityQueue.PRIORITIES, 7);
            message.setString("object-path", getTestName());
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 409);

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "READ");
            message.setObjectProperty("identity", identity);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            assertEquals("the queue did not have the correct number of priorities",
                         13,
                         Integer.valueOf(getValueFromMapResponse(responseMessage, PriorityQueue.PRIORITIES).toString())
                                .intValue());
            assertEquals("the queue did not have the expected path",
                         getTestName(),
                         getValueFromMapResponse(responseMessage, "object-path"));

            message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "UPDATE");
            message.setObjectProperty("identity", identity);
            message.setLong(ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES, 250L);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertEquals("The updated queue did not have the correct alerting threshold",
                         250L,
                         Long.valueOf(getValueFromMapResponse(responseMessage,
                                                              ALERT_THRESHOLD_QUEUE_DEPTH_MESSAGES).toString())
                             .longValue());

            message = _session.createMapMessage();
            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "DELETE");
            message.setObjectProperty("index", "object-path");
            message.setObjectProperty("key", path);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 204);

            message = _session.createMapMessage();
            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "DELETE");
            message.setObjectProperty("index", "object-path");
            message.setObjectProperty("key", path);

            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 404);
        }
        finally
        {
            connection.close();
        }
    }

    // read virtual host from virtual host management
    @Test
    public void testReadVirtualHost() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.VirtualHost");
            message.setStringProperty("operation", "READ");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertEquals("The name of the virtual host is not as expected",
                         getVirtualHostName(),
                         getValueFromMapResponse(responseMessage, "name"));

            message.setBooleanProperty("actuals", false);
            _producer.send(message);
            responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertNotNull("Derived attribute (productVersion) should be available",
                          getValueFromMapResponse(responseMessage, "productVersion"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testReadObject_ObjectNotFound() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Exchange");
            message.setStringProperty("operation", "READ");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "not-found-exchange");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 404);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testInvokeOperation_ObjectNotFound() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Exchange");
            message.setStringProperty("operation", "getStatistics");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "not-found-exchange");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 404);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testInvokeOperationReturningMap() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Broker");
            message.setStringProperty("operation", "getStatistics");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertNotNull(getValueFromMapResponse(responseMessage, "numberOfLiveThreads"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testInvokeOperationReturningManagedAttributeValue() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Broker");
            message.setStringProperty("operation", "getConnectionMetaData");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertNotNull(getValueFromMapResponse(responseMessage, "port"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testInvokeSecureOperation() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        String secureOperation = "publishMessage";  // // a secure operation
        Map<String, String> operationArg = new HashMap<>();
        operationArg.put("address", ExchangeDefaults.FANOUT_EXCHANGE_NAME);
        operationArg.put("content", "Hello, world!");

        Connection unsecuredConnection = getConnection();
        try
        {
            setUp(unsecuredConnection);

            MapMessage plainRequest = _session.createMapMessage();

            plainRequest.setStringProperty("type", "org.apache.qpid.VirtualHost");
            plainRequest.setStringProperty("operation", secureOperation);
            plainRequest.setStringProperty("index", "object-path");
            plainRequest.setStringProperty("key", "");
            plainRequest.setStringProperty("message", new ObjectMapper().writeValueAsString(operationArg));
            plainRequest.setJMSReplyTo(_replyAddress);
            _producer.send(plainRequest);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 403);
        }
        finally
        {
            unsecuredConnection.close();
        }

        int tlsPort = 0;
        final String portName = getTestName() + "TlsPort";
        final String keyStoreName = portName + "KeyStore";
        final String trustStoreName = portName + "TrustStore";
        try (final BrokerManagementHelper helper = new BrokerManagementHelper(getConnectionBuilder(),
                                                                              new AmqpManagementFacade(getProtocol())))
        {
            helper.openManagementConnection();

            final String authenticationManager =
                    helper.getAuthenticationProviderNameForAmqpPort(getBrokerAdmin().getBrokerAddress(
                            BrokerAdmin.PortType.AMQP)
                                                                                    .getPort());
            tlsPort = helper.createKeyStore(keyStoreName, _tlsHelper.getBrokerKeyStore(), TLS_RESOURCE.getSecret())
                            .createTrustStore(trustStoreName,
                                              _tlsHelper.getBrokerTrustStore(),
                                              TLS_RESOURCE.getSecret())
                            .createAmqpTlsPort(portName,
                                               authenticationManager,
                                               keyStoreName,
                                               false,
                                               false,
                                               false,
                                               trustStoreName).getAmqpBoundPort(portName);
        }

        Connection connection = getConnectionBuilder().setTls(true)
                                                      .setPort(tlsPort)
                                                      .setTrustStoreLocation(_tlsHelper.getClientTrustStore())
                                                      .setTrustStorePassword(TLS_RESOURCE.getSecret())
                                                      .build();
        try
        {
            setUp(connection);

            MapMessage secureRequest = _session.createMapMessage();

            secureRequest.setStringProperty("type", "org.apache.qpid.VirtualHost");
            secureRequest.setStringProperty("operation", secureOperation);
            secureRequest.setStringProperty("index", "object-path");
            secureRequest.setStringProperty("key", "");
            secureRequest.setStringProperty("message", new ObjectMapper().writeValueAsString(operationArg));
            secureRequest.setJMSReplyTo(_replyAddress);
            _producer.send(secureRequest);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
        }
        finally
        {
            connection.close();
        }
    }

    // create a virtual host from $management
    @Test
    public void testCreateVirtualHost() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        String virtualHostName = "newMemoryVirtualHost";
        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.JsonVirtualHostNode");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", virtualHostName);
            message.setString(VirtualHostNode.VIRTUALHOST_INITIAL_CONFIGURATION, "{ \"type\" : \"Memory\" }");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 201);
        }
        finally
        {
            connection.close();
        }

        Connection virtualHostConnection = getConnectionBuilder().setVirtualHost(virtualHostName).build();
        try
        {
            setUp(virtualHostConnection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.VirtualHost");
            message.setStringProperty("operation", "READ");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 200);
            checkResponseIsMapType(responseMessage);
            assertEquals("The name of the virtual host is not as expected",
                         virtualHostName,
                         getValueFromMapResponse(responseMessage, "name"));
            assertEquals("The type of the virtual host is not as expected",
                         "Memory",
                         getValueFromMapResponse(responseMessage, "qpid-type"));
        }
        finally
        {
            virtualHostConnection.close();
        }

    }

    // attempt to delete the virtual host via the virtual host
    @Test
    public void testDeleteVirtualHost() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.VirtualHost");
            message.setStringProperty("operation", "DELETE");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 501);
        }
        finally
        {
            connection.close();
        }
    }

    // create a queue with the qpid type
    @Test
    public void testCreateQueueWithQpidType() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Queue");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            message.setString("qpid-type", "lvq");
            String path = getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 201);
            checkResponseIsMapType(responseMessage);
            assertEquals("The created queue did not have the correct type",
                         "org.apache.qpid.LastValueQueue",
                         getValueFromMapResponse(responseMessage, "type"));
        }
        finally
        {
            connection.close();
        }
    }

    // create a queue using the AMQP type
    @Test
    public void testCreateQueueWithAmqpType() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.SortedQueue");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            String path = getTestName();
            message.setString("object-path", path);
            message.setString("sortKey", "foo");
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 201);
            checkResponseIsMapType(responseMessage);
            assertEquals("The created queue did not have the correct type",
                         "sorted",
                         getValueFromMapResponse(responseMessage, "qpid-type"));
        }
        finally
        {
            connection.close();
        }
    }

    // attempt to create an exchange without a type
    @Test
    public void testCreateExchangeWithoutType() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Exchange");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            String path = getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 400);
        }
        finally
        {
            connection.close();
        }
    }

    // attempt to create a connection
    @Test
    public void testCreateConnectionOnVirtualHostManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Connection");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            String path = getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 501);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testCreateConnectionOnBrokerManagement() throws Exception
    {
        assumeThat(isSupportedClient(), is(true));

        Connection connection = getBrokerManagementConnection();
        try
        {
            setUp(connection);

            MapMessage message = _session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.Connection");
            message.setStringProperty("operation", "CREATE");
            message.setString("name", getTestName());
            String path = getTestName();
            message.setString("object-path", path);
            message.setJMSReplyTo(_replyAddress);
            _producer.send(message);

            Message responseMessage = _consumer.receive(getReceiveTimeout());
            assertResponseCode(responseMessage, 501);
        }
        finally
        {
            connection.close();
        }
    }

    @SuppressWarnings("unchecked")
    private void assertResponseCode(final Message responseMessage, final int expectedResponseCode) throws JMSException
    {
        assertNotNull("A response message was not sent", responseMessage);
        assertTrue("The response message does not have a status code",
                   Collections.list(responseMessage.getPropertyNames()).contains("statusCode"));
        assertEquals("The response code did not indicate success",
                     expectedResponseCode, responseMessage.getIntProperty("statusCode"));
    }


    private Connection getBrokerManagementConnection() throws NamingException, JMSException
    {
        return getConnectionBuilder().setVirtualHost("$management")
                                     .setClientId(UUID.randomUUID().toString())
                                     .build();
    }

    private void checkResponseIsMapType(final Message responseMessage) throws JMSException
    {
        if (getProtocol() == Protocol.AMQP_1_0)
        {
            if (!(responseMessage instanceof MapMessage)
                && !(responseMessage instanceof ObjectMessage
                     && ((ObjectMessage) responseMessage).getObject() instanceof Map))
            {
                fail(String.format("The response was neither a Map Message nor an Object Message containing a Map. It was a : %s ",
                                   responseMessage.getClass()));
            }
        }
        else
        {
            assertTrue(String.format("The response was not a MapMessage. It was a '%s'.", responseMessage.getClass()), responseMessage instanceof MapMessage);
        }
    }

    private Object getValueFromMapResponse(final Message responseMessage, String name) throws JMSException
    {
        if (getProtocol() == Protocol.AMQP_1_0 && responseMessage instanceof ObjectMessage)
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).get(name);
        }
        else
        {
            return ((MapMessage) responseMessage).getObject(name);
        }
    }

    @SuppressWarnings("unchecked")
    private Collection<String> getMapResponseKeys(final Message responseMessage) throws JMSException
    {
        if (getProtocol() == Protocol.AMQP_1_0 && responseMessage instanceof ObjectMessage)
        {
            return ((Map)((ObjectMessage)responseMessage).getObject()).keySet();
        }
        else
        {
            return Collections.list(((MapMessage) responseMessage).getMapNames());
        }
    }

    private boolean isSupportedClient() throws NamingException, JMSException
    {
        if (getProtocol() == Protocol.AMQP_1_0)
        {
            return true;
        }
        else
        {
            Connection con = getConnection();
            try
            {
                final ConnectionMetaData metaData = con.getMetaData();
                // Older Qpid JMS Client 0-x (<=6.1.x) didn't support management addresses.
                return !(metaData.getProviderMajorVersion() < 6 || (metaData.getProviderMajorVersion() == 6
                                                                        && metaData.getProviderMinorVersion() <= 1));
            }
            finally
            {
                con.close();
            }
        }

    }
}
