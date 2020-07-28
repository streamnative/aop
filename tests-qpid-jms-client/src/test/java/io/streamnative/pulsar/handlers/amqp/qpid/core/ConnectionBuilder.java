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

import java.util.Map;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.naming.NamingException;

/**
 * ConnectionBuilder.
 */
public interface ConnectionBuilder
{
    String USERNAME = "guest";
    String PASSWORD = "guest";

    ConnectionBuilder setHost(String host);
    ConnectionBuilder setPort(int port);

    ConnectionBuilder setPrefetch(int prefetch);
    ConnectionBuilder setClientId(String clientId);
    ConnectionBuilder setUsername(String username);
    ConnectionBuilder setPassword(String password);
    ConnectionBuilder setVirtualHost(String virtualHostName);
    ConnectionBuilder setFailover(boolean enableFailover);
    ConnectionBuilder addFailoverPort(int port);
    ConnectionBuilder setFailoverReconnectAttempts(int reconnectAttempts);
    ConnectionBuilder setFailoverReconnectDelay(int connectDelay);
    ConnectionBuilder setTls(boolean enableTls);
    ConnectionBuilder setSyncPublish(boolean syncPublish);

    @Deprecated
    ConnectionBuilder setOptions(Map<String, String> options);
    ConnectionBuilder setPopulateJMSXUserID(boolean populateJMSXUserID);
    ConnectionBuilder setMessageRedelivery(final boolean redelivery);
    ConnectionBuilder setDeserializationPolicyWhiteList(String whiteList);
    ConnectionBuilder setDeserializationPolicyBlackList(String blackList);
    ConnectionBuilder setKeyStoreLocation(String keyStoreLocation);
    ConnectionBuilder setKeyStorePassword(String keyStorePassword);
    ConnectionBuilder setTrustStoreLocation(String trustStoreLocation);
    ConnectionBuilder setTrustStorePassword(String trustStorePassword);
    ConnectionBuilder setVerifyHostName(boolean verifyHostName);
    ConnectionBuilder setKeyAlias(String alias);
    ConnectionBuilder setSaslMechanisms(String... mechanism);
    ConnectionBuilder setCompress(boolean compress);

    Connection build() throws NamingException, JMSException;
    ConnectionFactory buildConnectionFactory() throws NamingException;
    String buildConnectionURL();

    ConnectionBuilder setTransport(String transport);
}
