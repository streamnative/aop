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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

/**
 * This class is used to transfer data between proxy and broker.
 */
@Slf4j
public class ProxyBrokerConnection {

    @Getter
    private Channel channel;
    @Getter
    private final Channel clientChannel;
    private List<ByteBuf> connectionCommands;
    private Map<Integer, ClientChannelMethodProcessor> clientProcessorMap;
    @Getter
    private ProxyClientConnection proxyConnection;
    @Getter
    private Map<Integer, ChannelState> channelState = new ConcurrentHashMap<>();
    @Getter
    private boolean isClose = false;

    public ProxyBrokerConnection(String host, Integer port, Channel clientChannel, List<ByteBuf> connectionCommands,
                                 ProxyClientConnection proxyConnection)
            throws InterruptedException {
        this.connectionCommands = connectionCommands;
        this.clientChannel = clientChannel;
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        log.info("channel pipeline {}", ch.pipeline());
                        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                                1000, true));
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("processor", new ProxyBrokerProcessor(clientChannel));
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
        this.clientProcessorMap = new HashMap<>();
        this.proxyConnection = proxyConnection;
    }

    protected enum ChannelState {
        OPEN,
        CLOSE
    }

    private class ProxyBrokerProcessor extends ChannelInboundHandlerAdapter
            implements ClientMethodProcessor<ClientChannelMethodProcessor> {

        private ChannelHandlerContext ctx;
        private final AmqpClientDecoder clientDecoder;

        public ProxyBrokerProcessor(Channel clientChannel) {
            clientDecoder = new AmqpClientDecoder(this, clientChannel);
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            super.channelActive(ctx);
            this.ctx = ctx;
            ChannelFuture future = null;
            for (ByteBuf command : connectionCommands) {
                command.retain();
                future = ctx.channel().writeAndFlush(command);
            }
            if (future != null) {
                future.addListener(future1 -> {
                    ctx.channel().read();
                });
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            clientDecoder.decodeBuffer(((ByteBuf) msg).nioBuffer());
        }

        public synchronized void writeFrame(AMQDataBlock frame) {
            if (log.isDebugEnabled()) {
                log.debug("Write data to broker by proxy, frame: {}.", frame);
            }
            this.ctx.writeAndFlush(frame);
        }

        @Override
        public void receiveConnectionStart(short versionMajor, short versionMinor, FieldTable serverProperties,
                                           byte[] mechanisms, byte[] locales) {
        }

        @Override
        public void receiveConnectionSecure(byte[] challenge) {
        }

        @Override
        public void receiveConnectionRedirect(AMQShortString host, AMQShortString knownHosts) {
        }

        @Override
        public void receiveConnectionTune(int channelMax, long frameMax, int heartbeat) {
        }

        @Override
        public void receiveConnectionOpenOk(AMQShortString knownHosts) {
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            return ProtocolVersion.v0_91;
        }

        @Override
        public ClientChannelMethodProcessor getChannelMethodProcessor(int channelId) {
            return clientProcessorMap.computeIfAbsent(channelId,
                    __ -> new AmqpProxyClientChannel(channelId, ProxyBrokerConnection.this));
        }

        @Override
        public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
            for (ProxyBrokerConnection conn : proxyConnection.getConnectionMap().values()) {
                conn.getChannel().writeAndFlush(
                        new ConnectionCloseBody(getProtocolVersion(), replyCode, replyText, classId, methodId)
                                .generateFrame(0));
            }
        }

        @Override
        public void receiveConnectionCloseOk() {
            isClose = true;
            boolean allClose = true;
            for (ProxyBrokerConnection conn : proxyConnection.getConnectionMap().values()) {
                if (!conn.isClose()) {
                    allClose = false;
                    break;
                }
            }
            if (allClose) {
                proxyConnection.writeFrame(ConnectionCloseOkBody.CONNECTION_CLOSE_OK_0_9.generateFrame(0));
            }
        }

        @Override
        public void receiveHeartbeat() {
            log.info("receiveHeartbeat");
        }

        @Override
        public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
            log.info("receiveProtocolHeader");
        }

        @Override
        public void setCurrentMethod(int classId, int methodId) {
            log.info("setCurrentMethod");
        }

        @Override
        public boolean ignoreAllButCloseOk() {
            return false;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            super.channelInactive(ctx);
            isClose = true;
            boolean allClose = true;
            for (ProxyBrokerConnection conn : proxyConnection.getConnectionMap().values()) {
                if (!conn.isClose()) {
                    allClose = false;
                    break;
                } else {
                    conn.channel.close();
                }
            }
            if (allClose) {
                proxyConnection.getCtx().close();
            }
        }
    }

}
