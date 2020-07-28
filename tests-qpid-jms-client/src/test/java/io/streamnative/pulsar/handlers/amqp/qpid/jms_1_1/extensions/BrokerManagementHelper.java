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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.AmqpManagementFacade;
import io.streamnative.pulsar.handlers.amqp.qpid.core.ConnectionBuilder;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.NamingException;
import org.apache.qpid.server.model.AuthenticationProvider;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Port;
import org.apache.qpid.server.model.User;
import org.apache.qpid.server.security.FileKeyStore;
import org.apache.qpid.server.security.FileTrustStore;
import org.apache.qpid.server.security.auth.manager.ExternalAuthenticationManager;

/**
 * BrokerManagementHelper.
 */
public class BrokerManagementHelper implements Closeable
{
    private final ConnectionBuilder _connectionBuilder;
    private final AmqpManagementFacade _managementFacade;
    private Connection _connection;

    public BrokerManagementHelper(final ConnectionBuilder connectionBuilder,
                                  final AmqpManagementFacade managementFacade)
    {
        _connectionBuilder = connectionBuilder;
        _managementFacade = managementFacade;
    }

    public BrokerManagementHelper openManagementConnection() throws JMSException, NamingException
    {
        _connection = _connectionBuilder.setVirtualHost("$management").build();
        _connection.start();
        return this;
    }

    public BrokerManagementHelper createKeyStore(final String keyStoreName,
                                                 final String keyStoreLocation,
                                                 final String keyStorePassword)
            throws JMSException
    {
        final Map<String, Object> keyStoreAttributes = new HashMap<>();
        keyStoreAttributes.put("storeUrl", keyStoreLocation);
        keyStoreAttributes.put("password", keyStorePassword);
        keyStoreAttributes.put("keyStoreType", java.security.KeyStore.getDefaultType());
        return createEntity(keyStoreName, FileKeyStore.class.getName(), keyStoreAttributes);
    }

    public BrokerManagementHelper createTrustStore(final String trustStoreName,
                                                   final String trustStoreLocation,
                                                   final String trustStorePassword) throws JMSException
    {
        final Map<String, Object> trustStoreAttributes = new HashMap<>();
        trustStoreAttributes.put("storeUrl", trustStoreLocation);
        trustStoreAttributes.put("password", trustStorePassword);
        trustStoreAttributes.put("trustStoreType", java.security.KeyStore.getDefaultType());
        return createEntity(trustStoreName, FileTrustStore.class.getName(), trustStoreAttributes);
    }

    public BrokerManagementHelper createAmqpTlsPort(final String portName,
                                                    final String authenticationProvider,
                                                    final String keyStoreName,
                                                    final boolean plainAndSsl,
                                                    final boolean needClientAuth,
                                                    final boolean wantClientAuth,
                                                    final String... trustStoreName) throws JMSException
    {
        try
        {
            final Map<String, Object> sslPortAttributes = new HashMap<>();
            sslPortAttributes.put(Port.TRANSPORTS, plainAndSsl ? "[\"SSL\",\"TCP\"]" : "[\"SSL\"]");
            sslPortAttributes.put(Port.PORT, 0);
            sslPortAttributes.put(Port.AUTHENTICATION_PROVIDER, authenticationProvider);
            sslPortAttributes.put(Port.NEED_CLIENT_AUTH, needClientAuth);
            sslPortAttributes.put(Port.WANT_CLIENT_AUTH, wantClientAuth);
            sslPortAttributes.put(Port.NAME, portName);
            sslPortAttributes.put(Port.KEY_STORE, keyStoreName);
            sslPortAttributes.put(Port.TRUST_STORES, new ObjectMapper().writeValueAsString(trustStoreName));
            createEntity(portName, "org.apache.qpid.AmqpPort", sslPortAttributes);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException("Unexpected json processing exception", e);
        }

        return this;
    }

    public BrokerManagementHelper createExternalAuthenticationProvider(String providerName, boolean useFullDN)
            throws JMSException
    {
        final Map<String, Object> providerAttributes = new HashMap<>();
        providerAttributes.put("qpid-type", ExternalAuthenticationManager.PROVIDER_TYPE);
        providerAttributes.put(ExternalAuthenticationManager.ATTRIBUTE_USE_FULL_DN, useFullDN);
        return createEntity(providerName,
                            AuthenticationProvider.class.getName(),
                            providerAttributes);
    }


    public BrokerManagementHelper createAuthenticationProvider(final String providerName, final String providerType)
            throws JMSException
    {
        return createEntity(providerName,
                            AuthenticationProvider.class.getName(),
                            Collections.singletonMap("qpid-type", providerType));
    }

    public BrokerManagementHelper createUser(final String providerName,
                                             final String userName,
                                             final String userPassword)
            throws JMSException
    {
        final Map<String, Object> userAttributes = new HashMap<>();
        userAttributes.put("qpid-type", "managed");
        userAttributes.put(User.PASSWORD, userPassword);
        userAttributes.put("object-path", providerName);
        return createEntity(userName, User.class.getName(), userAttributes);
    }


    public BrokerManagementHelper createEntity(final String name,
                                               final String type,
                                               final Map<String, Object> attributes) throws JMSException
    {
        final Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {

            _managementFacade.createEntityAndAssertResponse(name, type, attributes, session);
        }
        finally
        {
            session.close();
        }
        return this;
    }

    public int getAmqpBoundPort(final String portName) throws JMSException
    {
        return (int) getEffectiveAttribute(portName, "org.apache.qpid.AmqpPort", "boundPort");
    }

    public Object getEffectiveAttribute(final String name, final String type, String attributeName) throws JMSException
    {
        final Map<String, Object> effectiveAttributes = getEffectiveAttributes(name, type);
        if (effectiveAttributes.containsKey(attributeName))
        {
            return effectiveAttributes.get(attributeName);
        }
        throw new RuntimeException(String.format("Attribute '%s' is not found", attributeName));
    }

    public Map<String, Object> getEffectiveAttributes(final String name, final String type) throws JMSException
    {
        final Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return _managementFacade.readEntityUsingAmqpManagement(session, type, name, false);
        }
        finally
        {
            session.close();
        }
    }

    protected List<Map<String, Object>> queryEntitiesUsingAmqpManagement(final String type)
            throws JMSException
    {
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            return _managementFacade.managementQueryObjects(session, type);
        }
        finally
        {
            session.close();
        }
    }

    public String getConnectionPrincipalByClientId(String portName, String clientId) throws JMSException
    {
        final List<Map<String, Object>> connections = queryEntitiesUsingAmqpManagement("org.apache.qpid.Connection");
        for (final Map<String, Object> connection : connections)
        {
            final String name = String.valueOf(connection.get(ConfiguredObject.NAME));

            final Map<String, Object> attributes =
                    getEffectiveAttributes(portName + "/" + name, "org.apache.qpid.Connection");
            if (attributes.get(org.apache.qpid.server.model.Connection.CLIENT_ID).equals(clientId))
            {
                return String.valueOf(attributes.get(org.apache.qpid.server.model.Connection.PRINCIPAL));
            }
        }
        return null;
    }


    public void close()
    {
        if (_connection != null)
        {
            try
            {
                _connection.close();
            }
            catch (JMSException e)
            {
                throw new RuntimeException("Failure to close JMS connection", e);
            }
        }
    }

    public String getAuthenticationProviderNameForAmqpPort(final int brokerPort)
            throws JMSException
    {
        String authenticationProvider = null;
        Session session = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try
        {
            List<Map<String, Object>> ports =
                    _managementFacade.managementQueryObjects(session, "org.apache.qpid.AmqpPort");
            for (Map<String, Object> port : ports)
            {
                String name = String.valueOf(port.get(Port.NAME));

                Session s = _connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                try
                {
                    Map<String, Object> attributes = _managementFacade.readEntityUsingAmqpManagement(s,
                                                                                                     "org.apache.qpid.AmqpPort",
                                                                                                     name,
                                                                                                     false);
                    if (attributes.get("boundPort").equals(brokerPort))
                    {
                        authenticationProvider = String.valueOf(attributes.get(Port.AUTHENTICATION_PROVIDER));
                        break;
                    }
                }
                finally
                {
                    s.close();
                }
            }
        }
        finally
        {
            session.close();
        }
        return authenticationProvider;
    }
}
