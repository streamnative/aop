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
package io.streamnative.pulsar.handlers.amqp.test;

import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for AMQP connection level methods.
 */
@Log4j2
public class AmqpConnectionMethodTest extends AmqpProtocolTestBase {

    @Test
    public void testConnectionStartOk() {
        ConnectionStartOkBody cmd = methodRegistry.createConnectionStartOkBody(null,
            AMQShortString.createAMQShortString(""), new byte[0], AMQShortString.createAMQShortString("en_US"));
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionTuneBody);
    }

    @Test
    public void testConnectionSecureOk() {
        testConnectionStartOk();
        final int maxFrameSize = 4 * 1024 * 1024;
        final int heartBeat = 60;
        final int maxNoOfChannels = 256;
        connection.setHeartBeat(heartBeat);
        connection.setMaxFrameSize(maxFrameSize);
        connection.setMaxChannels(maxNoOfChannels);

        // TODO after finish the security process then fix this test
//        ConnectionSecureOkBody cmd = methodRegistry.createConnectionSecureOkBody("".getBytes());
//        cmd.generateFrame(0).writePayload(toServerSender);
//        toServerSender.flush();
//        AMQBody response = (AMQBody) clientChannel.poll();
//        Assert.assertTrue(response instanceof ConnectionTuneBody);
//        ConnectionTuneBody connectionTuneBody = (ConnectionTuneBody) response;
//        Assert.assertTrue(connectionTuneBody.getChannelMax() == maxNoOfChannels);
//        Assert.assertTrue(connectionTuneBody.getFrameMax() == maxFrameSize);
//        Assert.assertTrue(connectionTuneBody.getHeartbeat() == heartBeat);
    }

    @Test
    public void testConnectionTuneOk() {
        testConnectionSecureOk();
        final int maxFrameSize = 4 * 1024;
        final int heartBeat = 120;
        final int maxNoOfChannels = 256;
        ConnectionTuneOkBody cmd = methodRegistry.
            createConnectionTuneOkBody(maxNoOfChannels, maxFrameSize, heartBeat);
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        Assert.assertTrue(connection.getMaxFrameSize() == maxFrameSize);
        Assert.assertTrue(connection.getHeartBeat() == heartBeat);
        Assert.assertTrue(connection.getMaxChannels() == maxNoOfChannels);
    }

    @Test
    public void testMaxFrameSizeException() {
        testConnectionSecureOk();
        final int maxFrameSize = 4 * 1024 * 1024 + 1;
        final int heartBeat = 120;
        final int maxNoOfChannels = 256;
        ConnectionTuneOkBody cmd = methodRegistry.createConnectionTuneOkBody(maxNoOfChannels, maxFrameSize, heartBeat);
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionCloseBody);
    }

    @Test
    public void testConnectionOpen() throws Exception {
        testConnectionTuneOk();
        String tenant = "public";
        String vhost = "vhost1";
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getAdminClient().namespaces().
            getPolicies(NamespaceName.get(tenant, vhost).toString())).thenReturn(new Policies());
        ConnectionOpenBody cmd = methodRegistry.createConnectionOpenBody(AMQShortString.createAMQShortString(vhost),
            AMQShortString.createAMQShortString("123"), false);
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionOpenOkBody);
    }

    @Test
    public void testConnectionClose() {
        ConnectionCloseBody cmd = methodRegistry.createConnectionCloseBody(404,
            AMQShortString.createAMQShortString("123"), 60, 30);
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionCloseOkBody);
    }

    @Test
    public void testChannelOpen() throws Exception {
        testConnectionOpen();
        ChannelOpenBody cmd = methodRegistry.createChannelOpenBody(AMQShortString.createAMQShortString(""));
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ChannelOpenOkBody);
    }

    @Test
    public void testChannelClose() throws Exception {
        testChannelOpen();
        ChannelCloseBody cmd = methodRegistry.createChannelCloseBody(404,
            AMQShortString.createAMQShortString("hi server,please close channel"), 60, 40);
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ChannelCloseOkBody);
    }

}
