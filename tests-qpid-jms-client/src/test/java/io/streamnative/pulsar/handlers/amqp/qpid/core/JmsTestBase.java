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
package io.streamnative.pulsar.handlers.amqp.qpid.core;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.getAmqpManagementFacade;
import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.getJmsProvider;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.naming.NamingException;
import java.net.InetSocketAddress;
import org.apache.qpid.server.model.Protocol;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jms test base.
 */
public abstract class JmsTestBase extends BrokerAdminUsingTestBase
{
    public static final String DEFAULT_BROKER_CONFIG = "classpath:config-jms-tests.json";

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsTestBase.class);
    private static JmsProvider _jmsProvider;
    private static AmqpManagementFacade _managementFacade;

    @BeforeClass
    public static void setUpTestBase()
    {
        _managementFacade = getAmqpManagementFacade();
        _jmsProvider = getJmsProvider();
        LOGGER.debug("Test receive timeout is {} milliseconds", getReceiveTimeout());
    }

    protected ConnectionBuilder getConnectionBuilder()
    {
        InetSocketAddress brokerAddress = getBrokerAdmin().getBrokerAddress(BrokerAdmin.PortType.AMQP);
        return _jmsProvider.getConnectionBuilder()
                           .setHost(brokerAddress.getHostName())
                           .setPort(brokerAddress.getPort())
                           .setUsername(getBrokerAdmin().getValidUsername())
                           .setPassword(getBrokerAdmin().getValidPassword());
    }

    protected void createEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _managementFacade.createEntityUsingAmqpManagement(entityName, session, entityType, attributes);
        }
        finally
        {
            connection.close();
        }
    }

    protected Object performOperationUsingAmqpManagement(final String name,
                                                         final String operation,
                                                         final String type,
                                                         Map<String, Object> arguments)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            return _managementFacade.performOperationUsingAmqpManagement(name, operation, session, type, arguments);
        }
        finally
        {
            connection.close();
        }
    }

    protected Connection getConnection() throws JMSException, NamingException
    {
        return getConnectionBuilder().build();
    }

    protected static long getReceiveTimeout()
    {
        return Utils.getReceiveTimeout();
    }

    protected String getVirtualHostName()
    {
        return "vhost1";
//        return getClass().getSimpleName() + "_" + getTestName();
    }

    protected Queue getQueue(String queueName) throws Exception
    {
        return _jmsProvider.getTestQueue(queueName);
    }

    protected Topic createTopic(final String topicName) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            return _jmsProvider.createTopic(connection, topicName);
        }
        finally
        {
            connection.close();
        }
    }

    protected Queue createQueue(final String queueName) throws Exception
    {
        return createQueue(getVirtualHostName(), queueName);
    }

    protected Queue createQueue(final String virtualHostName, final String queueName) throws Exception
    {
        Connection connection = getConnectionBuilder().setVirtualHost(virtualHostName).build();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                return _jmsProvider.createQueue(session, queueName);
            }
            finally
            {
                session.close();
            }
        }
        finally
        {
            connection.close();
        }
    }

    protected int getQueueCount() throws Exception
    {
        Map<String, Object> statisticsMap = getVirtualHostStatistics("queueCount");
        return ((Number) statisticsMap.get("queueCount")).intValue();
    }

    protected long getTotalDepthOfQueuesMessages() throws Exception
    {
        Map<String, Object> statisticsMap = getVirtualHostStatistics("totalDepthOfQueuesMessages");
        return ((Number) statisticsMap.get("totalDepthOfQueuesMessages")).intValue();
    }

    @SuppressWarnings("unchecked")
    protected Map<String, Object> getVirtualHostStatistics(final String... statisticsName) throws Exception
    {
        Map<String, Object> arguments = Collections.singletonMap("statistics", Arrays.asList(statisticsName));
        Object statistics = performOperationUsingAmqpManagement(getVirtualHostName(),
                                                                "getStatistics",
                                                                "org.apache.qpid.VirtualHost",
                                                                arguments);

        assertNotNull("Statistics is null", statistics);
        assertTrue("Statistics is not map", statistics instanceof Map);

        return (Map<String, Object>) statistics;
    }

    protected void updateEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType,
                                                   final Map<String, Object> attributes)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            updateEntityUsingAmqpManagement(entityName, entityType, attributes, connection);
        }
        finally
        {
            connection.close();
        }
    }

    protected void updateEntityUsingAmqpManagement(final String entityName,
                                                 final String entityType,
                                                 final Map<String, Object> attributes,
                                                 final Connection connection)
            throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            _managementFacade.updateEntityUsingAmqpManagementAndReceiveResponse(entityName, entityType, attributes, session);
        }
        finally
        {
            session.close();
        }
    }

    protected void deleteEntityUsingAmqpManagement(final String entityName,
                                                   final String entityType)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            _managementFacade.deleteEntityUsingAmqpManagement(entityName, session, entityType);
        }
        finally
        {
            connection.close();
        }
    }

    protected Map<String, Object> readEntityUsingAmqpManagement(String name, String type, boolean actuals)
            throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            return readEntityUsingAmqpManagement(name, type, actuals, connection);
        }
        finally
        {
            connection.close();
        }
    }

    protected Map<String, Object> readEntityUsingAmqpManagement(final String name,
                                                                final String type,
                                                                final boolean actuals,
                                                                final Connection connection)
            throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return _managementFacade.readEntityUsingAmqpManagement(session, type, name, actuals);
        }
        finally
        {
            session.close();
        }
    }

    protected List<Map<String, Object>> queryEntitiesUsingAmqpManagement(final String type, final Connection connection)
            throws JMSException
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return _managementFacade.managementQueryObjects(session, type);
        }
        finally
        {
            session.close();
        }
    }

    protected Map<String, Object> createEntity(final String entityName,
                                               final String entityType,
                                               final Map<String, Object> attributes, final Connection connection)
            throws Exception
    {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return _managementFacade.createEntityAndAssertResponse(entityName, entityType, attributes, session);
        }
        finally
        {
            session.close();
        }
    }

    protected TopicConnection getTopicConnection() throws JMSException, NamingException
    {
        return (TopicConnection) getConnection();
    }

    protected static Protocol getProtocol()
    {
        return Utils.getProtocol();
    }

    public QueueConnection getQueueConnection() throws JMSException, NamingException
    {
        return (QueueConnection)getConnection();
    }
}
