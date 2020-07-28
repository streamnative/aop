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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.Hashtable;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.util.Objects;

/**
 * QpidJmsClient0xProvider.
 */
public class QpidJmsClient0xProvider implements JmsProvider
{

    public QpidJmsClient0xProvider()
    {
    }

    @Override
    public Connection getConnection(String urlString) throws Exception
    {
        final Hashtable<Object, Object> initialContextEnvironment = new Hashtable<>();
        initialContextEnvironment.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jndi.PropertiesFileInitialContextFactory");
        final String factoryName = "connectionFactory";
        initialContextEnvironment.put("connectionfactory." + factoryName, urlString);
        ConnectionFactory connectionFactory =
                (ConnectionFactory) new InitialContext(initialContextEnvironment).lookup(factoryName);
        return connectionFactory.createConnection();
    }


    @Override
    public Queue getTestQueue(final String testQueueName) throws NamingException
    {
        return createReflectively("org.apache.qpid.client.AMQQueue", "amq.direct", testQueueName);
    }

    @Override
    public Queue getQueueFromName(Session session, String name) throws JMSException
    {
        return createReflectively("org.apache.qpid.client.AMQQueue", "", name);
    }

    @Override
    public Queue createQueue(Session session, String queueName) throws JMSException
    {

        Queue amqQueue;
        try
        {
            amqQueue = getTestQueue(queueName);
        }
        catch (NamingException e)
        {
            throw new RuntimeException(e);
        }
        session.createConsumer(amqQueue).close();
        return amqQueue;
    }

    @Override
    public Topic getTestTopic(final String testQueueName)
    {
        return createReflectively("org.apache.qpid.client.AMQTopic", "amq.topic", testQueueName);
    }

    @Override
    public Topic createTopic(final Connection con, final String topicName) throws JMSException
    {
        return getTestTopic(topicName);
    }

    @Override
    public Topic createTopicOnDirect(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return createReflectively("org.apache.qpid.client.AMQTopic",
                                  "direct://amq.direct/"
                                  + topicName
                                  + "/"
                                  + topicName
                                  + "?routingkey='"
                                  + topicName
                                  + "',exclusive='true',autodelete='true'");
    }

    private <T> T createReflectively(String className, Object ...args)
    {
        try
        {
            Class<?> topicClass = Class.forName(className);
            Class[] classes = new Class[args.length];
            for (int i = 0; i < args.length; ++i)
            {
                classes[i] = args[i].getClass();
            }
            Constructor<?> constructor = topicClass.getConstructor(classes);
            return (T) constructor.newInstance(args);
        }
        catch (IllegalAccessException | AccessControlException | InvocationTargetException | InstantiationException | NoSuchMethodException | ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }

    }

    @Override
    public Topic createTopicOnFanout(final Connection con, String topicName) throws JMSException, URISyntaxException
    {
        return createReflectively("org.apache.qpid.client.AMQTopic", "fanout://amq.fanout/"
                                                                     + topicName
                                                                     + "/"
                                                                     + topicName
                                                                     + "?routingkey='"
                                                                     + topicName
                                                                     + "',exclusive='true',autodelete='true'");
    }

    @Override
    public QpidJmsClient0xConnectionBuilder getConnectionBuilder()
    {
        return new QpidJmsClient0xConnectionBuilder();
    }

    @Override
    public void addGenericConnectionListener(final Connection connection,
                                             final GenericConnectionListener listener)
    {
        try
        {
            final Class<?> iface = Class.forName("org.apache.qpid.jms.ConnectionListener");
            final Object listenerProxy = Proxy.newProxyInstance(iface.getClassLoader(),
                                                                new Class[]{iface},
                                                                (proxy, method, args) -> {
                                                                    final String methodName = method.getName();
                                                                    switch (methodName)
                                                                    {
                                                                        case "preFailover":
                                                                        {
                                                                            URI uri = getConnectedURI(connection);
                                                                            listener.onConnectionInterrupted(uri);
                                                                            return true;
                                                                        }
                                                                        case "preResubscribe":
                                                                            return true;
                                                                        case "failoverComplete":
                                                                        {
                                                                            URI uri = getConnectedURI(connection);
                                                                            listener.onConnectionRestored(uri);
                                                                            break;
                                                                        }
                                                                        case "toString":
                                                                        return String.format("[Proxy %s]",
                                                                                                 listener.toString());
                                                                        case "equals":
                                                                            Object other = args[0];
                                                                            return Objects.equals(this, other);
                                                                        case "hashCode":
                                                                            return Objects.hashCode(this);
                                                                    }
                                                                    return null;
                                                                });

            final Method setConnectionListener = connection.getClass().getMethod("setConnectionListener", iface);
            setConnectionListener.invoke(connection, listenerProxy);
        }
        catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException("Unable to reflectively add listener", e);
        }
    }

    @Override
    public URI getConnectedURI(final Connection connection)
    {
        try
        {
            final Method brokerDetailsMethod = connection.getClass().getMethod("getActiveBrokerDetails");
            Object abd =  brokerDetailsMethod.invoke(connection);
            final Method getHostMethod = abd.getClass().getMethod("getHost");
            final Method getPortMethod = abd.getClass().getMethod("getPort");
            final Method getTransportMethod = abd.getClass().getMethod("getTransport");
            String host = (String) getHostMethod.invoke(abd);
            int port = (Integer) getPortMethod.invoke(abd);
            String transport = (String) getTransportMethod.invoke(abd);
            return URI.create(String.format("%s://%s:%d", transport, host, port));
        }
        catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e)
        {
            throw new RuntimeException("Unable to reflectively get connected URI", e);
        }
    }
}
