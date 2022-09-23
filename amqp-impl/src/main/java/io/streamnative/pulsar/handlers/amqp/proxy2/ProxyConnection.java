package io.streamnative.pulsar.handlers.amqp.proxy2;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * Proxy connection, it used to manage RabbitMQ client connection
 * One connection will maintain multi connection with broker
 */
@Slf4j
public class ProxyConnection extends ChannelInboundHandlerAdapter
        implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    private final ProxyConfiguration config;
    private final Map<Long, ProxyChannel> channelMap = new ConcurrentHashMap<>();
    private final Map<String, ProxyConnection> connectionMap = new ConcurrentHashMap<>();
    private final Map<Integer, ProxyServerChannel> serverChannelMap = new ConcurrentHashMap<>();
    private final List<ByteBuf> connectionCommands = new ArrayList<>();
    private final AmqpBrokerDecoder decoder;
    private State state;
    private final ProtocolVersion protocolVersion;
    private final MethodRegistry methodRegistry;
    private ChannelHandlerContext ctx;
    private final PulsarServiceLookupHandler lookupHandler;
    private String vhost;

    enum State {
        INIT,
        CONNECTED,
        FAILED
    }

    public ProxyConnection(ProxyConfiguration config, PulsarServiceLookupHandler lookupHandler) {
        this.config = config;
        this.decoder = new AmqpBrokerDecoder(this);
        this.state = State.INIT;
        protocolVersion = ProtocolVersion.v0_91;
        methodRegistry = new MethodRegistry(protocolVersion);
        this.lookupHandler = lookupHandler;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        log.info("xxxx [Netty] channelActive");
        this.ctx = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        log.info("xxxx [Netty] channelRead");
        ByteBuf byteBuf = (ByteBuf) msg;
        if (state.equals(State.INIT)) {
            connectionCommands.add(byteBuf);
        }
        try {
            decoder.decodeBuffer(QpidByteBuffer.wrap(byteBuf.nioBuffer()));
        } catch (Exception e) {
            this.state = State.FAILED;
            log.error("Failed to decode requests", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("xxxx [Netty] Exception caught in channel", cause);
        this.state = State.FAILED;
    }

    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response, AMQShortString locale) {
        log.info("xxxx [ProxyServer] receiveConnectionStartOk");
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                        config.getAmqpMaxFrameSize(), config.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        log.info("xxxx [ProxyServer] receiveConnectionSecureOk");
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                        config.getAmqpMaxFrameSize(), config.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
        log.info("xxxx [ProxyServer] receiveConnectionTuneOk");
    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        log.info("xxxx [ProxyServer] receiveConnectionOpen");
        this.state = State.CONNECTED;

        String virtualHostStr = AMQShortString.toString(virtualHost);
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/') {
            virtualHostStr = virtualHostStr.substring(1);
            if (StringUtils.isEmpty(virtualHostStr)){
                virtualHostStr = "default";
            }
        }
        this.vhost = virtualHostStr;

        ConnectionOpenOkBody okBody =
                methodRegistry.createConnectionOpenOkBody(virtualHost);
        writeFrame(okBody.generateFrame(0));
    }

    @Override
    public void receiveChannelOpen(int channelId) {
        log.info("xxxx [ProxyServer] receiveChannelOpen channelId: {}", channelId);
        ProxyServerChannel channel = new ProxyServerChannel(channelId, this);
        serverChannelMap.put(channelId, channel);
        ChannelOpenOkBody okBody =
                methodRegistry.createChannelOpenOkBody();
        writeFrame(okBody.generateFrame(channelId));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        log.info("xxxx [ProxyServer] getProtocolVersion");
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        log.info("xxxx [ProxyServer] getChannelMethodProcessor");
        return serverChannelMap.get(channelId);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        log.info("xxxx [ProxyServer] receiveConnectionClose");
    }

    @Override
    public void receiveConnectionCloseOk() {
        log.info("xxxx [ProxyServer] receiveConnectionCloseOk");
    }

    @Override
    public void receiveHeartbeat() {
        log.info("xxxx [ProxyServer] receiveHeartbeat");
        writeFrame(HeartbeatBody.FRAME);
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        log.info("xxxx [ProxyServer] receiveProtocolHeader");
        decoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            // TODO serverProperties mechanis
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                    protocolVersion.getMajorVersion(),
                    pv.getActualMinorVersion(),
                    null,
                    // TODO temporary modification
                    "PLAIN token".getBytes(US_ASCII),
                    "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (QpidException e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
        }
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        if (classId != 0 && methodId != 0) {
            log.info("xxxx [ProxyServer] setCurrentMethod classId: {}, methodId:{}", classId, methodId);
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
//        log.info("xxxx [ProxyServer] ignoreAllButCloseOk");
        return false;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isInfoEnabled()) {
            log.info("xxxx [ProxyServer] writeFrame: " + frame);
        }
        this.ctx.writeAndFlush(frame);
    }

    public CompletableFuture<ProxyBrokerConnection> getBrokerConnection(String topic) {
        CompletableFuture<Pair<String, Integer>> future =
                lookupHandler.findBroker(
                        TopicName.get("persistent://public/" + vhost + "/" + topic),
                        AmqpProtocolHandler.PROTOCOL_NAME);
        return future.thenApply(pair -> {
            try {
                return new ProxyBrokerConnection(pair.getLeft(), pair.getRight(), ctx.channel());
            } catch (InterruptedException e) {
                log.error("Failed to create proxy broker connection with address {}:{}",
                        pair.getLeft(), pair.getRight(), e);
                throw new RuntimeException(e);
            }
        });
    }

}
