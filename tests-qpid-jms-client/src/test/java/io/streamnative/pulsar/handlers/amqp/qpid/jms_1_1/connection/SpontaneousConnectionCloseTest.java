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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.connection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import com.google.common.util.concurrent.SettableFuture;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.model.Protocol;
import org.junit.Before;
import org.junit.Test;

/**
 * SpontaneousConnectionCloseTest.
 */
public class SpontaneousConnectionCloseTest extends JmsTestBase
{
    private SettableFuture<JMSException> _connectionCloseFuture;

    @Before
    public void setUp() throws Exception
    {
        _connectionCloseFuture = SettableFuture.create();
    }

    @Test
    public void explictManagementConnectionClose() throws Exception
    {
        assumeThat(getBrokerAdmin().isManagementSupported(), is(true));

        Connection con = getConnection();
        try
        {
            con.setExceptionListener(_connectionCloseFuture::set);

            final UUID uuid = getConnectionUUID(con);

            closeConnectionUsingAmqpManagement(uuid);

            assertClientConnectionClosed(con);
        }
        finally
        {
            con.close();
        }
    }

    @Test
    public void brokerRestartConnectionClose() throws Exception
    {
        Connection con = getConnectionBuilder().setFailover(false).build();
        try
        {
            con.setExceptionListener(_connectionCloseFuture::set);

            getBrokerAdmin().restart();

            assertClientConnectionClosed(con);
        }
        finally
        {
            con.close();
        }
    }

    private void assertClientConnectionClosed(final Connection con) throws Exception
    {
        _connectionCloseFuture.get(getReceiveTimeout(), TimeUnit.MILLISECONDS);
        try
        {
            con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            fail("Connection close ought to invalidate further use");
        }
        catch (JMSException e)
        {
            // PASS
        }
    }

    private UUID getConnectionUUID(final Connection connection) throws Exception
    {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MapMessage message = session.createMapMessage();

        message.setStringProperty("type", "org.apache.qpid.VirtualHost");
        message.setStringProperty("operation", "getConnectionMetaData");
        message.setStringProperty("index", "object-path");
        message.setStringProperty("key", "");

        MapMessage response = (MapMessage) sendManagementRequestAndGetResponse(session, message, 200);

        return UUID.fromString(String.valueOf(response.getObject("connectionId")));
    }

    private void closeConnectionUsingAmqpManagement(final UUID targetConnectionId) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MapMessage message = session.createMapMessage();
            message.setStringProperty("operation", "DELETE");
            message.setStringProperty("type", "org.apache.qpid.Connection");
            message.setStringProperty("identity", String.valueOf(targetConnectionId));

            sendManagementRequestAndGetResponse(session, message, 204);
        }
        finally
        {
            connection.close();
        }
    }

    private Message sendManagementRequestAndGetResponse(final Session session,
                                                        final MapMessage request,
                                                        final int expectedResponseCode) throws Exception
    {
        final Queue queue;
        final Queue replyConsumer;
        Queue replyAddress;

        if (getProtocol() == Protocol.AMQP_1_0)
        {
            queue = session.createQueue("$management");
            replyAddress = session.createTemporaryQueue();
            replyConsumer = replyAddress;
        }
        else
        {
            queue = session.createQueue("ADDR:$management");
            replyAddress = session.createQueue("ADDR:!response");
            replyConsumer = session.createQueue(
                    "ADDR:$management ; {assert : never, node: { type: queue }, link:{name: \"!response\"}}");
        }
        request.setJMSReplyTo(replyAddress);

        final MessageConsumer consumer = session.createConsumer(replyConsumer);
        final MessageProducer producer = session.createProducer(queue);

        producer.send(request);

        final Message responseMessage = consumer.receive(getReceiveTimeout());
        assertThat("The response code did not indicate success",
                   responseMessage.getIntProperty("statusCode"), is(equalTo(expectedResponseCode)));


        return responseMessage;
    }
}
