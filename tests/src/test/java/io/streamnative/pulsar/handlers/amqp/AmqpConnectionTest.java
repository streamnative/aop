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
package io.streamnative.pulsar.handlers.amqp;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;

import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarService;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for AmqpConnection.
 */
@Log4j2
public class AmqpConnectionTest {

    private ByteBufferSender toServerSender;
    private MethodRegistry methodRegistry;
    private AmqpClientChannel clientChannel;

    @BeforeClass
    public void setup() throws Exception {
        AmqpConnection connection = new MockConnection();
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctx.channel()).thenReturn(Mockito.mock(Channel.class));
        connection.channelActive(ctx);
        connection.getBrokerDecoder().setExpectProtocolInitiation(false);
        toServerSender = new ToServerByteBufferSender(connection);
        methodRegistry = new MethodRegistry(ProtocolVersion.v0_91);
        clientChannel = new AmqpClientChannel();
        connection.setBufferSender(new ToClientByteBufferSender(new ClientDecoder
                (new AmqpClientMethodProcessor(clientChannel))));
    }

    @Test
    public void testConnectionStart() throws Exception {
        initProtocol();
        AMQMethodBody response = clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionStartBody);
        ConnectionStartBody connectionStartBody = (ConnectionStartBody) response;
        Assert.assertEquals(connectionStartBody.getVersionMajor(), 0);
        Assert.assertEquals(connectionStartBody.getVersionMinor(), 9);
    }

    @Test
    public void testConnectionStartOk() throws Exception {
        initProtocol();
        ConnectionStartOkBody cmd = methodRegistry.createConnectionStartOkBody(Mockito.mock(FieldTable.class),
                AMQShortString.createAMQShortString(""), new byte[0], AMQShortString.createAMQShortString("en_US"));
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
    }

    @Test
    public void testBasicGet() throws Exception {
        initProtocol();
        clientChannel.poll();
        BasicGetBody cmd = methodRegistry.createBasicGetBody(1, AMQShortString.createAMQShortString("test"), false);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQMethodBody response = clientChannel.poll();
        Assert.assertTrue(response instanceof BasicGetOkBody);
        BasicGetOkBody basicGetOkBody = (BasicGetOkBody) response;
        Assert.assertEquals(basicGetOkBody.getMessageCount(), 100);
    }

    private void initProtocol() {
        ProtocolInitiation initiation = new ProtocolInitiation(ProtocolVersion.v0_91);
        initiation.writePayload(toServerSender);
        toServerSender.flush();
    }

    private static class MockConnection extends AmqpConnection {

        private MockChannel channelMethodProcessor;

        public MockConnection() {
            super(Mockito.mock(PulsarService.class), Mockito.mock(AmqpServiceConfiguration.class));
            this.channelMethodProcessor = new MockChannel(this);
        }

        @Override
        public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
            return channelMethodProcessor;
        }
    }

    private static class MockChannel extends AmqpChannel {

        public MockChannel(AmqpConnection serverMethodProcessor) {
            super(serverMethodProcessor);
        }

        @Override
        public void receiveBasicGet(AMQShortString queue, boolean noAck) {
            if (log.isDebugEnabled()) {
                log.debug("RECV BasicGet[queue={}, noAck={}]", queue, noAck);
            }
            try {
                AMQMethodBody res = connection.getMethodRegistry().createBasicGetOkBody(1, true, AMQShortString.createAMQShortString("default"), AMQShortString.createAMQShortString(""), 100);
                connection.writeFrame(res.generateFrame(1));
            } catch (Exception e) {
                log.error("FAILED BasicGet", e);
            }
        }
    }
}
