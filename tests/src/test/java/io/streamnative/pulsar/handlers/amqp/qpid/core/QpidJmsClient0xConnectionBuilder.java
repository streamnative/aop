/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package io.streamnative.pulsar.handlers.amqp.qpid.core;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class QpidJmsClient0xConnectionBuilder implements ConnectionBuilder
{
    private final List<Integer> _failoverPorts = new ArrayList<>();

    private String _clientId = "clientid";
    private String _username = USERNAME;
    private String _password = PASSWORD;
    private String _virtualHost;
    private boolean _enableTls;
    private boolean _enableFailover;
    private final Map<String, Object> _options = new TreeMap<>();
    private int _reconnectAttempts = 20;
    private int _connectdelay;
    private String _host = "localhost";
    private int _port;
    private String _keyStoreLocation;
    private String _keyStorePassword;
    private String _trustStoreLocation;
    private String _trustStorePassword;
    private Boolean _verifyHostName;
    private String _keyAlias;
    private String _saslMechanisms;

    @Override
    public ConnectionBuilder setHost(final String host)
    {
        _host = host;
        return this;
    }

    @Override
    public ConnectionBuilder setPort(final int port)
    {
        _port = port;
        return this;
    }

    @Override
    public ConnectionBuilder addFailoverPort(final int port)
    {
        _failoverPorts.add(port);
        return this;
    }

    @Override
    public ConnectionBuilder setPrefetch(final int prefetch)
    {
        _options.put("maxprefetch", prefetch);
        return this;
    }

    @Override
    public ConnectionBuilder setClientId(final String clientId)
    {
        _clientId = clientId;
        return this;
    }

    @Override
    public ConnectionBuilder setUsername(final String username)
    {
        _username = username;
        return this;
    }

    @Override
    public ConnectionBuilder setPassword(final String password)
    {
        _password = password;
        return this;
    }

    @Override
    public ConnectionBuilder setVirtualHost(final String virtualHostName)
    {
        _virtualHost = virtualHostName;
        return this;
    }

    @Override
    public ConnectionBuilder setFailover(final boolean enableFailover)
    {
        _enableFailover = enableFailover;
        return this;
    }

    @Override
    public ConnectionBuilder setFailoverReconnectAttempts(final int reconnectAttempts)
    {
        _reconnectAttempts = reconnectAttempts;
        return this;
    }

    @Override
    public ConnectionBuilder setFailoverReconnectDelay(final int connectDelay)
    {
        _connectdelay = connectDelay;
        return this;
    }

    @Override
    public ConnectionBuilder setTls(final boolean enableTls)
    {
        _enableTls = enableTls;
        return this;
    }

    @Override
    public ConnectionBuilder setSyncPublish(final boolean syncPublish)
    {
        if (syncPublish)
        {
            _options.put("sync_publish", "all");
        }
        else
        {
            _options.remove("sync_publish");
        }
        return this;
    }

    @Override
    public ConnectionBuilder setOptions(final Map<String, String> options)
    {
        _options.putAll(options);
        return this;
    }

    @Override
    public ConnectionBuilder setPopulateJMSXUserID(final boolean populateJMSXUserID)
    {
        _options.put("populateJMSXUserID", String.valueOf(populateJMSXUserID));
        return this;
    }

    @Override
    public ConnectionBuilder setMessageRedelivery(final boolean redelivery)
    {
        if (redelivery)
        {
            _options.put("rejectbehaviour", "server");
        }
        else
        {
            _options.remove("rejectbehaviour");
        }
        return this;
    }

    @Override
    public ConnectionBuilder setDeserializationPolicyWhiteList(final String whiteList)
    {
        _options.put("objectMessageClassHierarchyWhiteList", whiteList);
        return this;
    }

    @Override
    public ConnectionBuilder setDeserializationPolicyBlackList(final String blackList)
    {
        _options.put("objectMessageClassHierarchyBlackList", blackList);
        return this;
    }

    @Override
    public ConnectionBuilder setKeyStoreLocation(final String keyStoreLocation)
    {
        _keyStoreLocation = keyStoreLocation;
        return this;
    }

    @Override
    public ConnectionBuilder setKeyStorePassword(final String keyStorePassword)
    {
        _keyStorePassword = keyStorePassword;
        return this;
    }

    @Override
    public ConnectionBuilder setTrustStoreLocation(final String trustStoreLocation)
    {
        _trustStoreLocation = trustStoreLocation;
        return this;
    }

    @Override
    public ConnectionBuilder setTrustStorePassword(final String trustStorePassword)
    {
        _trustStorePassword = trustStorePassword;
        return this;
    }

    @Override
    public ConnectionBuilder setVerifyHostName(final boolean verifyHostName)
    {
        _verifyHostName = verifyHostName;
        return this;
    }

    @Override
    public ConnectionBuilder setKeyAlias(final String alias)
    {
        _keyAlias = alias;
        return this;
    }

    @Override
    public ConnectionBuilder setSaslMechanisms(final String... mechanism)
    {
        _saslMechanisms = String.join(" ", mechanism);
        return this;
    }

    @Override
    public ConnectionBuilder setCompress(final boolean compress)
    {
        _options.put("compressMessages", compress);
        return this;
    }

    @Override
    public Connection build() throws JMSException, NamingException
    {
        return buildConnectionFactory().createConnection(_username, _password);
    }

    @Override
    public ConnectionFactory buildConnectionFactory() throws NamingException
    {
        String connectionUrl = buildConnectionURL();
        log.info("connectionUrl: {}", connectionUrl);

        System.setProperty("qpid.amqp.version", "0-9-1");

        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY,
                                      "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        initialContextEnvironment.put("connectionfactory." + factoryName, connectionUrl);
        InitialContext initialContext = new InitialContext(initialContextEnvironment);
        try
        {
            return (ConnectionFactory) initialContext.lookup(factoryName);
        }
        finally
        {
            initialContext.close();
        }
    }

    @Override
    public String buildConnectionURL()
    {
        StringBuilder cUrlBuilder = new StringBuilder("amqp://");
        if (_username != null)
        {
            cUrlBuilder.append(_username);
        }

        if (_username != null || _password != null)
        {
            cUrlBuilder.append(":");
        }

        if (_password != null)
        {
            cUrlBuilder.append(_password);
        }

        if (_username != null || _password != null)
        {
            cUrlBuilder.append("@");
        }

        if (_clientId != null)
        {
            cUrlBuilder.append(_clientId);
        }

        cUrlBuilder.append("/");

        if (_virtualHost != null)
        {
            cUrlBuilder.append(_virtualHost);
        }
        cUrlBuilder.append("?");

        final List<Integer> copy = new ArrayList<>(_failoverPorts.size() + 1);
        copy.add(_port);

        if (_enableFailover)
        {
            copy.addAll(_failoverPorts);
        }

        final String transportQuery = buildTransportQuery();
        final String brokerlist = copy.stream()
                                    .map(port -> String.format("tcp://%s:%d%s", _host, port, transportQuery))
                                    .collect(Collectors.joining(";", "brokerlist='", "'"));
        cUrlBuilder.append(brokerlist);

        if (_enableFailover)
        {
            cUrlBuilder.append("&sync_ack='true'&sync_publish='all'");
            cUrlBuilder.append(String.format("&failover='roundrobin?cyclecount='%d''", _reconnectAttempts));
        }

        if (_saslMechanisms != null)
        {
            cUrlBuilder.append("&");
            cUrlBuilder.append("sasl_mechs='").append(_saslMechanisms).append('\'');
        }

        for (Map.Entry<String, Object> entry : _options.entrySet())
        {
            cUrlBuilder.append("&").append(entry.getKey()).append("='").append(entry.getValue()).append("'");
        }
        return cUrlBuilder.toString();
    }

    @Override
    public ConnectionBuilder setTransport(final String transport)
    {
        throw new UnsupportedOperationException("Cannot modify transport");
    }

    private String buildTransportQuery()
    {
        final StringBuilder builder = new StringBuilder();

        if (_enableTls)
        {
            builder.append("?ssl='true'");
            if (_keyStoreLocation != null)
            {
                builder.append("&key_store='").append(encodeBrokerOption(_keyStoreLocation)).append('\'');
            }
            if (_keyStorePassword != null)
            {
                builder.append("&key_store_password='").append(_keyStorePassword).append('\'');
            }
            if (_trustStoreLocation != null)
            {
                builder.append("&trust_store='").append(encodeBrokerOption(_trustStoreLocation)).append('\'');
            }
            if (_trustStorePassword != null)
            {
                builder.append("&trust_store_password='").append(_trustStorePassword).append('\'');
            }
            if (_verifyHostName != null)
            {
                builder.append("&ssl_verify_hostname='").append(_verifyHostName).append('\'');
            }
            if (_keyAlias != null)
            {
                builder.append("&ssl_cert_alias='").append(_keyAlias).append('\'');
            }
        }
        if (_connectdelay > 0)
        {
            final char initial = builder.length() == 0 ? '?' : '&';
            builder.append(String.format("%cconnectdelay='%d'", initial, _connectdelay));
        }

        return builder.toString();
    }

    private String encodeBrokerOption(final String canonicalPath)
    {
        try
        {
            return URLEncoder.encode(encodeConnectionOption(canonicalPath), StandardCharsets.UTF_8.name());
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    private String encodeConnectionOption(final String canonicalPath)
    {
        try
        {
            return URLEncoder.encode(canonicalPath, StandardCharsets.UTF_8.name()).replace("+", "%20");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }
}
