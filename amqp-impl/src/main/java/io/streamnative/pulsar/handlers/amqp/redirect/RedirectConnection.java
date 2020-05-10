package io.streamnative.pulsar.handlers.amqp.redirect;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.US_ASCII;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.swagger.models.auth.In;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.AuthData;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Redirect connection.
 */
@Slf4j
public class RedirectConnection extends ChannelInboundHandlerAdapter implements
        ServerMethodProcessor<ServerChannelMethodProcessor> {

    private RedirectService redirectService;
    private RedirectConfiguration redirectConfig;
    @Getter
    private ChannelHandlerContext cnx;
    private State state;
    private NamespaceName namespaceName;
    private int amqpBrokerPort = 5672;
    private String amqpBrokerHost;
    private RedirectHandler redirectHandler;

    protected AmqpBrokerDecoder brokerDecoder;
    private MethodRegistry methodRegistry;
    private ProtocolVersion protocolVersion;
    private int currentClassId;
    private int currentMethodId;

    private List<Object> connectMsgList = Lists.newArrayList();

    private enum State {
        Init,
        RedirectLookup,
        RedirectToBroker,
        Close
    }

    public RedirectConnection(RedirectService redirectService) {
        this.redirectService = redirectService;
        this.redirectConfig = redirectService.getRedirectConfig();
        brokerDecoder = new AmqpBrokerDecoder(this);
        protocolVersion = ProtocolVersion.v0_91;
        methodRegistry = new MethodRegistry(protocolVersion);
        state = State.Init;
    }

    @Override
    public void channelActive(ChannelHandlerContext cnx) throws Exception {
        super.channelActive(cnx);
        this.cnx = cnx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (state) {
            case Init:
            case RedirectLookup:
                log.info("RedirectConnection [channelRead] - RedirectLookup");
                // Get a buffer that contains the full frame
                ByteBuf buffer = (ByteBuf) msg;

                io.netty.channel.Channel nettyChannel = ctx.channel();
                checkState(nettyChannel.equals(this.cnx.channel()));

                try {
                    brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
                } catch (Throwable e) {
                    log.error("error while handle command:", e);
                    close();
                }
                brokerDecoder.getMethodProcessor();

                connectMsgList.add(msg);
                break;
            case RedirectToBroker:
                log.info("RedirectConnection [channelRead] - RedirectToBroker");
                redirectHandler.getBrokerChannel().writeAndFlush(msg);
        }
    }

    // step 1
    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        if (log.isDebugEnabled()) {
            log.debug("RedirectConnection - [receiveProtocolHeader] Protocol Header [{}]", protocolInitiation);
        }
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            // TODO serverProperties mechanis
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                    (short) protocolVersion.getMajorVersion(),
                    (short) pv.getActualMinorVersion(),
                    null,
                    // TODO temporary modification
                    "PLAIN".getBytes(US_ASCII),
                    "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (QpidException e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
        }
    }

    // step 2
    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
                                         AMQShortString locale) {
        if (log.isDebugEnabled()) {
            log.debug("RedirectConnection - [receiveConnectionStartOk] " +
                            "clientProperties: {}, mechanism: {}, locale: {}", clientProperties, mechanism, locale);
        }
        AMQMethodBody responseBody = this.methodRegistry.createConnectionSecureBody(new byte[0]);
        writeFrame(responseBody.generateFrame(0));
    }

    // step 3
    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        if (log.isDebugEnabled()) {
            log.debug("RedirectConnection - [receiveConnectionSecureOk] response: {}", new String(response));
        }
        // TODO AUTH
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(redirectConfig.getMaxNoOfChannels(),
                        redirectConfig.getMaxFrameSize(), redirectConfig.getHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    // step 4
    @Override
    public void receiveConnectionTuneOk(int i, long l, int i1) {
        log.info("RedirectConnection - [receiveConnectionTuneOk]");
    }

    // step 5
    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        log.info("RedirectConnection - [receiveConnectionOpen]");
        if (log.isDebugEnabled()) {
            log.debug("RedirectConnection - [receiveConnectionOpen] virtualHost: {} capabilities: {} insist: {}",
                    virtualHost, capabilities, insist);
        }

        state = State.RedirectLookup;
        String virtualHostStr = AMQShortString.toString(virtualHost);
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/') {
            virtualHostStr = virtualHostStr.substring(1);
        }

        if (redirectService.getVhostBrokerMap().containsKey(virtualHostStr)) {
            amqpBrokerHost = redirectService.getVhostBrokerMap().get(virtualHostStr);
        } else {
            NamespaceName namespaceName = NamespaceName.get(redirectConfig.getAmqpTenant(), virtualHostStr);
            this.namespaceName = namespaceName;

            try {
                Pair<InetSocketAddress, InetSocketAddress> pair =
                        this.redirectService.getBrokerDiscoveryProvider().lookupBroker(namespaceName);
                log.info("logical address: {}, physical address: {}", pair.getLeft(), pair.getRight());
                InetSocketAddress logicalAddress = pair.getLeft();
                InetSocketAddress physicalAddress = pair.getRight();
                amqpBrokerHost = logicalAddress.getHostString();
            } catch (Exception e) {
                log.error("Failed to lookup broker.", e);
            }
        }

        try {
            redirectHandler = new RedirectHandler(redirectService,
                    this, amqpBrokerHost, amqpBrokerPort, connectMsgList);
            redirectService.getVhostBrokerMap().put(virtualHostStr, amqpBrokerHost);
            state = State.RedirectToBroker;

            AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHost);
            writeFrame(responseBody.generateFrame(0));
        } catch (Exception e) {
            log.error("Failed to lookup broker.", e);
        }
    }

    @Override
    public void receiveChannelOpen(int i) {
        log.info("RedirectConnection - [receiveChannelOpen]");
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        log.info("RedirectConnection - [getProtocolVersion]");
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int i) {
        log.info("RedirectConnection - [getChannelMethodProcessor]");
        return null;
    }

    @Override
    public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {
        log.info("RedirectConnection - [receiveConnectionClose]");
    }

    @Override
    public void receiveConnectionCloseOk() {
        log.info("RedirectConnection - [receiveConnectionCloseOk]");
    }

    @Override
    public void receiveHeartbeat() {
        log.info("RedirectConnection - [receiveHeartbeat]");
    }


    @Override
    public void setCurrentMethod(int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("RedirectConnection - [setCurrentMethod] classId: {}, methodId: {}", classId, methodId);
        }
        currentClassId = classId;
        currentMethodId = methodId;
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        log.info("RedirectConnection - [ignoreAllButCloseOk]");
        return false;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("send: " + frame);
        }
        cnx.writeAndFlush(frame);
    }

    public String getAmqpBrokerUrl() {
        return amqpBrokerHost + ":" + amqpBrokerPort;
    }

    public void close() {

    }

}
