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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.util.concurrent.DefaultEventExecutor;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerService;
import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.AmqpPulsarServerCnx;
import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import io.streamnative.pulsar.handlers.amqp.AmqpTopicManager;
import io.streamnative.pulsar.handlers.amqp.ConnectionContainer;
import io.streamnative.pulsar.handlers.amqp.test.mock.MockDispatcher;
import io.streamnative.pulsar.handlers.amqp.test.mock.MockManagedLedger;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorContainer;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

/**
 * Base test for AMQP protocol tests.
 */
@Log4j2
public abstract class AmqpProtocolTestBase {

    private BrokerService brokerService;
    private PulsarService pulsarService;
    protected AmqpConnection connection;
    protected ByteBufferSender toServerSender;
    protected MethodRegistry methodRegistry;
    protected AmqpClientChannel clientChannel;

    protected AmqpTopicManager amqpTopicManager;
    protected AmqpBrokerService amqpBrokerService;

    public static byte[] contentBytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    @BeforeMethod
    public void setup() throws Exception {
        mockPulsarService();
        mockBrokerService();
        ConnectionContainer connectionContainer = Mockito.mock(ConnectionContainer.class);
        amqpBrokerService = new AmqpBrokerService(pulsarService, connectionContainer);
        amqpTopicManager = amqpBrokerService.getAmqpTopicManager();

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
        connection.setBufferSender(new ToClientByteBufferSender(new AmqpClientDecoder
            (new AmqpClientMethodProcessor(clientChannel))));
        AmqpPulsarServerCnx serverCnx = new AmqpPulsarServerCnx(connection.getPulsarService(), ctx);
        connection.setPulsarServerCnx(serverCnx);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);
        initProtocol();
        initMockAmqpTopicManager();
        initDefaultExchange();
    }

    private void mockPulsarService() throws PulsarServerException {
        pulsarService = Mockito.mock(PulsarService.class);
        PulsarAdmin adminClient = Mockito.mock(PulsarAdmin.class);
        Namespaces namespaces = Mockito.mock(Namespaces.class);
        ServiceConfiguration serviceConfiguration = Mockito.mock(ServiceConfiguration.class);
        Mockito.when(pulsarService.getAdminClient()).thenReturn(adminClient);
        Mockito.when(pulsarService.getAdminClient().namespaces()).thenReturn(namespaces);
        Mockito.when(pulsarService.getBrokerService()).then(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return brokerService;
            }
        });
        Mockito.when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);
        Mockito.when(pulsarService.getOrderedExecutor()).thenReturn(
                OrderedExecutor.newBuilder().numThreads(8).name("pulsar-ordered").build());
    }

    private void mockBrokerService() {
        brokerService = Mockito.mock(BrokerService.class);
        Mockito.when(brokerService.executor()).thenReturn(new DefaultEventExecutor());
        Mockito.when(brokerService.pulsar()).thenReturn(pulsarService);
    }

    private void initDefaultExchange() {
        String tenant = "public";
        String namespace = "vhost1";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
    }


    private void initMockAmqpTopicManager(){
        CompletableFuture<Topic> completableFuture = new CompletableFuture<>();
        PersistentTopic persistentTopic = Mockito.mock(PersistentTopic.class);

        CompletableFuture<Subscription> subFuture = new CompletableFuture<>();
        Subscription subscription = Mockito.mock(Subscription.class);
        subFuture.complete(subscription);
        Mockito.when(persistentTopic.createSubscription(Mockito.anyString(),
                Mockito.any(), Mockito.anyBoolean())).thenReturn(subFuture);
        Mockito.when(subscription.getDispatcher()).thenReturn(Mockito.mock(MockDispatcher.class));
        Mockito.when(persistentTopic.getSubscriptions()).thenReturn(new ConcurrentOpenHashMap<>());
        Mockito.when(persistentTopic.getManagedLedger()).thenReturn(new MockManagedLedger());
        Mockito.when(persistentTopic.getBrokerService()).thenReturn(brokerService);
        CompletableFuture deleteCpm = new CompletableFuture<>();
        Mockito.when(persistentTopic.delete()).thenReturn(deleteCpm);
        deleteCpm.complete(null);

        completableFuture.complete(persistentTopic);
        NamespaceService namespaceService = Mockito.mock(NamespaceService.class);

        CompletableFuture<Optional<LookupResult>> lookupCompletableFuture = new CompletableFuture<>();
        LookupResult lookupResult = Mockito.mock(LookupResult.class);
        lookupCompletableFuture.complete(Optional.of(lookupResult));
        Mockito.when(connection.getPulsarService().getNamespaceService()).thenReturn(namespaceService);
        Mockito.when(namespaceService.getBrokerServiceUrlAsync(Mockito.any(TopicName.class),
                Mockito.any(LookupOptions.class))).then(invocationOnMock -> {
            Object[] args = invocationOnMock.getArguments();
            Mockito.when(persistentTopic.getName()).thenReturn(args[0].toString());
            return lookupCompletableFuture;
        });

        LookupData lookupData = Mockito.mock(LookupData.class);
        Mockito.when(lookupResult.getLookupData()).thenReturn(lookupData);
        Mockito.when(lookupResult.getLookupData().getBrokerUrl()).thenReturn("127.0.0.1");
        CompletableFuture<Optional<Topic>> topicCompletableFuture = new CompletableFuture<>();
        topicCompletableFuture.complete(Optional.of(persistentTopic));

        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(brokerService);
        Mockito.when(brokerService.getTopic(Mockito.anyString(), Mockito.anyBoolean())).
                thenReturn(topicCompletableFuture);

        ManagedLedger managedLedger = Mockito.mock(ManagedLedgerImpl.class);
        Mockito.when(persistentTopic.getManagedLedger()).thenReturn(managedLedger);
        Mockito.when(managedLedger.getCursors()).thenReturn(new ManagedCursorContainer());
    }

    /**
     * Mock AMQP connection for tests.
     */
    private class MockConnection extends AmqpConnection {

        private MockChannel channelMethodProcessor;

        public MockConnection() throws PulsarServerException {
            super(Mockito.mock(AmqpServiceConfiguration.class), amqpBrokerService);
            this.channelMethodProcessor = new MockChannel(0, this);
        }

        @Override
        public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
            return channelMethodProcessor;
        }

        @Override public synchronized void writeFrame(AMQDataBlock frame) {
            if (log.isDebugEnabled()) {
                log.debug("send: " + frame);
            }
            frame.writePayload(getBufferSender());
            getBufferSender().flush();
        }
    }

    /**
     * Mock AMQP channel for tests.
     */
    private class MockChannel extends AmqpChannel {

        public MockChannel(int channelId, AmqpConnection serverMethodProcessor) {
            super(channelId, serverMethodProcessor, amqpBrokerService);
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
        AMQBody response = (AMQBody) clientChannel.poll(1, TimeUnit.SECONDS);
        Assert.assertTrue(response instanceof ConnectionStartBody);
        ConnectionStartBody connectionStartBody = (ConnectionStartBody) response;
        Assert.assertEquals(connectionStartBody.getVersionMajor(), 0);
        Assert.assertEquals(connectionStartBody.getVersionMinor(), 9);
    }
}
