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
package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import static java.nio.charset.StandardCharsets.US_ASCII;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
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
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

/**
 * Proxy connection, it used to manage RabbitMQ client connection.
 */
@Slf4j
public class ProxyClientConnection extends ChannelInboundHandlerAdapter
        implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    private final ProxyConfiguration config;
    @Getter
    private final Map<String, ProxyBrokerConnection> connectionMap = new ConcurrentHashMap<>();
    @Getter
    private final Map<Integer, AmqpProxyServerChannel> serverChannelMap = new ConcurrentHashMap<>();
    private final List<ByteBuf> connectionCommands = new ArrayList<>();
    private final AmqpBrokerDecoder decoder;
    private State state;
    private final ProtocolVersion protocolVersion;
    private final MethodRegistry methodRegistry;
    @Getter
    private ChannelHandlerContext ctx;
    private final PulsarServiceLookupHandler lookupHandler;
    @Getter
    private String vhost;
    @Getter
    private final ProxyServiceV2 proxyServer;
    @Getter
    private int currentClassId;
    @Getter
    private int currentMethodId;

    enum State {
        INIT,
        CONNECTED,
        FAILED
    }

    public ProxyClientConnection(ProxyConfiguration config,
                                 PulsarServiceLookupHandler lookupHandler,
                                 ProxyServiceV2 proxyServer) {
        this.config = config;
        this.decoder = new AmqpBrokerDecoder(this);
        this.state = State.INIT;
        protocolVersion = ProtocolVersion.v0_91;
        methodRegistry = new MethodRegistry(protocolVersion);
        this.lookupHandler = lookupHandler;
        this.proxyServer = proxyServer;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection channel active.");
        }
        this.ctx = ctx;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf byteBuf = (ByteBuf) msg;
        if (state.equals(State.INIT)) {
            byteBuf.retain();
            connectionCommands.add(byteBuf);
        }
        try {
            decoder.decodeBuffer(QpidByteBuffer.wrap(byteBuf.nioBuffer()));
        } catch (Exception e) {
            this.state = State.FAILED;
            log.error("ProxyClientConnection failed to decode requests.", e);
        } finally {
            byteBuf.release();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("ProxyClientConnection exception caught in channel", cause);
        this.state = State.FAILED;
    }

    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
                                         AMQShortString locale) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive connection start request, clientProperties: {}, mechanism: {}, "
                    + "response: {}, locale: {}.", clientProperties, mechanism, response, locale);
        }
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                        config.getAmqpMaxFrameSize(), config.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive connection secure ok request, response: {}.", response);
        }
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(config.getAmqpMaxNoOfChannels(),
                        config.getAmqpMaxFrameSize(), config.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive connection tune ok request, channelMax: {}, frameMax: {}, "
                    + "heartbeat: {}.", channelMax, frameMax, heartbeat);
        }
    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive connection open request, virtualHost: {}, capabilities: {}, "
                    + "insist: {}.", virtualHost, capabilities, insist);
        }
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
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive channel open request, channelId: {}.", channelId);
        }
        AmqpProxyServerChannel channel = new AmqpProxyServerChannel(channelId, this);
        serverChannelMap.put(channelId, channel);
        ChannelOpenOkBody okBody =
                methodRegistry.createChannelOpenOkBody();
        writeFrame(okBody.generateFrame(channelId));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection get protocol version.");
        }
        return ProtocolVersion.v0_91;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection get channel method processor, channelId: {}.", channelId);
        }
        return serverChannelMap.get(channelId);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive connection close request, replyCode: {}, replyText: {}, "
                            + "classId: {}, methodId: {}.", replyCode, replyText, classId, methodId);
        }
        for (ProxyBrokerConnection conn : getConnectionMap().values()) {
            conn.getChannel().writeAndFlush(
                    new ConnectionCloseBody(
                            getProtocolVersion(), replyCode, replyText, classId, methodId).generateFrame(0));
        }
    }

    @Override
    public void receiveConnectionCloseOk() {
        // nothing to do
    }

    @Override
    public void receiveHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive heart beat request.");
        }
        for (ProxyBrokerConnection conn : getConnectionMap().values()) {
            conn.getChannel().writeAndFlush(new HeartbeatBody());
        }
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection receive protocol header request, protocolInitiation: {}",
                    protocolInitiation);
        }
        decoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                    protocolVersion.getMajorVersion(),
                    pv.getActualMinorVersion(),
                    null,
                    "PLAIN token".getBytes(US_ASCII),
                    "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (QpidException e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
            writeFrame(new ProtocolInitiation(ProtocolVersion.v0_91));
            throw new ProxyServiceV2Exception(e);
        }
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        if (classId != 0 && methodId != 0) {
            this.currentClassId = classId;
            this.currentMethodId = methodId;
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientConnection write frame: {}", frame);
        }
        this.ctx.writeAndFlush(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        for (ByteBuf command : connectionCommands) {
            ReferenceCountUtil.safeRelease(command);
        }
    }

    public void sendChannelClose(int channelId, int replyCode, final String replyText) {
        this.ctx.writeAndFlush(new ChannelCloseBody(replyCode, AMQShortString.createAMQShortString(replyText),
                currentClassId, currentMethodId).generateFrame(channelId));
    }

    public void sendConnectionClose(int replyCode, String replyText) {
        this.ctx.writeAndFlush(new ConnectionCloseBody(getProtocolVersion(), replyCode,
                AMQShortString.createAMQShortString(replyText), currentClassId, currentMethodId).generateFrame(0));
    }

    public CompletableFuture<ProxyBrokerConnection> getBrokerConnection(String topic) {
        CompletableFuture<Pair<String, Integer>> future =
                lookupHandler.findBroker(
                        TopicName.get("persistent://public/" + vhost + "/" + topic),
                        AmqpProtocolHandler.PROTOCOL_NAME);
        return future.thenApply(pair -> connectionMap.computeIfAbsent(
                pair.getLeft() + ":" + pair.getRight(), __ -> {
            try {
                return new ProxyBrokerConnection(
                        pair.getLeft(), pair.getRight(), ctx.channel(), connectionCommands, ProxyClientConnection.this);
            } catch (InterruptedException e) {
                log.error("Failed to create proxy broker connection with address {}:{} for topic {}",
                        pair.getLeft(), pair.getRight(), topic, e);
                return null;
            }
        }));
    }

    public void closeChannelAndWriteFrame(AmqpChannel channel, int cause, String message) {
        writeFrame(new ChannelCloseBody(cause, AMQShortString.validValueOf(message), currentClassId, currentMethodId)
                .generateFrame(channel.getChannelId()));
        closeChannel(channel, true);
    }

    void closeChannel(AmqpChannel channel, boolean mark) {
        int channelId = channel.getChannelId();
        try {
            channel.close();
            if (mark) {
//                markChannelAwaitingCloseOk(channelId);
            }
        } finally {
            removeChannel(channelId);
        }
    }

//    private void markChannelAwaitingCloseOk(int channelId) {
//        closingChannelsList.put(channelId, System.currentTimeMillis());
//    }

    private synchronized void removeChannel(int channelId) {
        serverChannelMap.remove(channelId);
    }

}
