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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.AmqpPulsarServerCnx;
import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import io.streamnative.pulsar.handlers.amqp.AmqpTopicManager;
import io.streamnative.pulsar.handlers.amqp.test.frame.AmqpClientChannel;
import io.streamnative.pulsar.handlers.amqp.test.frame.AmqpClientMethodProcessor;
import io.streamnative.pulsar.handlers.amqp.test.frame.ClientDecoder;
import io.streamnative.pulsar.handlers.amqp.test.frame.ToClientByteBufferSender;
import io.streamnative.pulsar.handlers.amqp.test.frame.ToServerByteBufferSender;
import io.streamnative.pulsar.handlers.amqp.test.mock.MockDispatcher;
import io.streamnative.pulsar.handlers.amqp.test.mock.MockManagedLedger;
import io.streamnative.pulsar.handlers.amqp.test.mock.MockTopic;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;



/**
 * Base test for AMQP protocol tests.
 */
@Log4j2
public abstract class AmqpProtocolTestBase {

    protected AmqpConnection connection;
    protected ByteBufferSender toServerSender;
    protected MethodRegistry methodRegistry;
    protected AmqpClientChannel clientChannel;

    public static byte[] contentBytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    @BeforeMethod
    public void setup() throws Exception {
        // 1.Init AMQP connection for connection methods and channel methods tests.
        connection = new MockConnection();
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);
        Channel channel = Mockito.mock(Channel.class);
        SocketAddress socketAddress = Mockito.mock(SocketAddress.class);
        Mockito.when(ctx.channel()).thenReturn(channel);
        Mockito.when(channel.remoteAddress()).thenReturn(socketAddress);
        Mockito.when(ctx.pipeline()).thenReturn(Mockito.mock(ChannelPipeline.class));
        connection.channelActive(ctx);

        // 2.Init ByteBuffer sender for the test to send requests to AMQP server.
        toServerSender = new ToServerByteBufferSender(connection);

        // 3.Init method registry for convenient request creation
        methodRegistry = new MethodRegistry(ProtocolVersion.v0_91);

        // 4.Init client channel to get response from AMQP server.
        clientChannel = new AmqpClientChannel();

        // 5.Buffer sender in connection sends response to the client. So set a new ToClientByteBufferSender for
        //   the connection. So that the response ByteBuffer from the AMQP server sends to the client by
        //   ToClientByteBufferSender and decodes by ClientDecoder, then the decoder add decoded protocol body to the
        //   client channel. So, we can get a response from the client channel straightforward.
        connection.setBufferSender(new ToClientByteBufferSender(new ClientDecoder
            (new AmqpClientMethodProcessor(clientChannel))));
        AmqpPulsarServerCnx serverCnx = new AmqpPulsarServerCnx(connection.getPulsarService(), ctx);
        connection.setPulsarServerCnx(serverCnx);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);
        initProtocol();
    }

    /**
     * Mock AMQP connection for tests.
     */
    private class MockConnection extends AmqpConnection {

        private MockChannel channelMethodProcessor;

        public MockConnection() throws PulsarServerException {
            super(Mockito.mock(PulsarService.class), Mockito.mock(AmqpServiceConfiguration.class),
                Mockito.mock(AmqpTopicManager.class));

            PulsarAdmin adminClient = Mockito.mock(PulsarAdmin.class);
            Namespaces namespaces = Mockito.mock(Namespaces.class);
            BrokerService brokerService = Mockito.mock(BrokerService.class);
            ServiceConfiguration serviceConfiguration = Mockito.mock(ServiceConfiguration.class);
            Mockito.when(getPulsarService().getAdminClient()).thenReturn(adminClient);
            Mockito.when(getPulsarService().getAdminClient().namespaces()).thenReturn(namespaces);
            Mockito.when(getPulsarService().getBrokerService()).thenReturn(brokerService);
            Mockito.when(brokerService.pulsar()).thenReturn(getPulsarService());
            Mockito.when(getPulsarService().getConfiguration()).thenReturn(serviceConfiguration);
//            Mockito.when(serviceConfiguration.get).thenReturn(serviceConfiguration);
            PersistentTopic persistentTopic = Mockito.mock(PersistentTopic.class);
            CompletableFuture<Subscription> subFuture = new CompletableFuture<>();
            Subscription subscription = Mockito.mock(Subscription.class);
            subFuture.complete(subscription);
            Mockito.when(persistentTopic.createSubscription(anyString(), any(), anyBoolean())).thenReturn(subFuture);
            Mockito.when(subscription.getDispatcher()).thenReturn(Mockito.mock(MockDispatcher.class));
            Mockito.when(persistentTopic.getName()).thenReturn("persistent://public/default/mock");

            AmqpTopicManager amqpTopicManager = Mockito.mock(AmqpTopicManager.class);
            CompletableFuture<PersistentTopic> completableFuture = new CompletableFuture<>();
            completableFuture.complete(persistentTopic);
            Mockito.when(amqpTopicManager.getTopic(anyString())).thenReturn(completableFuture);
            Mockito.when(amqpTopicManager.getOrCreateTopic(anyString(), anyBoolean())).thenReturn(new MockTopic());
            Mockito.when(persistentTopic.getManagedLedger()).thenReturn(new MockManagedLedger());
            super.setAmqpTopicManager(amqpTopicManager);
            this.channelMethodProcessor = new MockChannel(0, this);
        }

        @Override
        public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
            return channelMethodProcessor;
        }

    }

    /**
     * Mock AMQP channel for tests.
     */
    private class MockChannel extends AmqpChannel {

        public MockChannel(int channelId, AmqpConnection serverMethodProcessor) {
            super(channelId, serverMethodProcessor);
        }

        @Override
        public void receiveBasicGet(AMQShortString queue, boolean noAck) {
            if (log.isDebugEnabled()) {
                log.debug("RECV BasicGet[queue={}, noAck={}]", queue, noAck);
            }
            try {
                AMQMethodBody res = connection.getMethodRegistry().createBasicGetOkBody(1, true,
                    AMQShortString.createAMQShortString("default"), AMQShortString.createAMQShortString(""), 100);
                connection.writeFrame(res.generateFrame(1));
            } catch (Exception e) {
                log.error("FAILED BasicGet", e);
            }
        }
    }

    /**
     * Before test connection methods and channel methods, client should send protocol header to AMQP server. Otherwise,
     * the server decoder will skip all other methods. Also can get around by {@code
     * connection.getBrokerDecoder().setExpectProtocolInitiation(false)}.
     */
    protected void initProtocol() {
        ProtocolInitiation initiation = new ProtocolInitiation(ProtocolVersion.v0_91);
        initiation.writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionStartBody);
        ConnectionStartBody connectionStartBody = (ConnectionStartBody) response;
        Assert.assertEquals(connectionStartBody.getVersionMajor(), 0);
        Assert.assertEquals(connectionStartBody.getVersionMinor(), 9);
    }
}
