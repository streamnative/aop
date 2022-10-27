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
package io.streamnative.pulsar.handlers.amqp.proxy;

import static com.google.common.base.Preconditions.checkState;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

/**
 * Proxy handler is the bridge between client and broker.
 */
@Slf4j
public class ProxyHandler {

    private String vhost;
    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    private Channel clientChannel;
    @Getter
    private Channel brokerChannel;
    private State state;
    private List<Object> connectMsgList;

    ProxyHandler(String vhost, ProxyService proxyService, ProxyConnection proxyConnection,
                 String amqpBrokerHost, int amqpBrokerPort, List<Object> connectMsgList,
                 AMQMethodBody responseBody) throws Exception {
        this.proxyService = proxyService;
        this.proxyConnection = proxyConnection;
        this.clientChannel = this.proxyConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;
        this.vhost = vhost;
        this.state = State.Init;

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                                proxyService.getProxyConfig().getAmqpExplicitFlushAfterFlushes(), true));
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("processor", new ProxyBackendHandler(responseBody));
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(amqpBrokerHost, amqpBrokerPort);
        brokerChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                proxyConnection.close();
            }
        });
        log.info("Broker channel connect. vhost: {}, broker: {}:{}, isOpen: {}",
                vhost, amqpBrokerHost, amqpBrokerPort, brokerChannel.isOpen());
    }

    private class ProxyBackendHandler extends ChannelInboundHandlerAdapter implements
            ClientMethodProcessor<ClientChannelMethodProcessor>, FutureListener<Void> {

        private ChannelHandlerContext cnx;
        private final AMQMethodBody connectResponseBody;
        private final AmqpClientDecoder clientDecoder;

        ProxyBackendHandler(AMQMethodBody responseBody) {
            this.connectResponseBody = responseBody;
            clientDecoder = new AmqpClientDecoder(this);
        }

        @Override
        public void operationComplete(Future future) throws Exception {
            // This is invoked when the write operation on the paired connection
            // is completed
            if (future.isSuccess()) {
                cnx.channel().read();
            } else {
                log.warn("[{}] [{}] Failed to write on proxy connection. Closing both connections.", clientChannel,
                        cnx.channel(), future.cause());
                proxyConnection.close();
            }

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("[{}] ProxyBackendHandler [channelActive]", vhost);
            this.cnx = ctx;
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                ((ByteBuf) msg).retain();
                ctx.channel().writeAndFlush(msg).addListener(future -> {
                    ctx.channel().read();
                });
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (log.isDebugEnabled()) {
                log.debug("[{}] ProxyBackendHandler [channelRead]", vhost);
            }
            switch (state) {
                case Init:
                case Failed:
                    // Get a buffer that contains the full frame
                    ByteBuf buffer = (ByteBuf) msg;

                    io.netty.channel.Channel nettyChannel = ctx.channel();
                    checkState(nettyChannel.equals(this.cnx.channel()));

                    try {
                        clientDecoder.decodeBuffer(buffer.nioBuffer());
                    } catch (Throwable e) {
                        log.error("error while handle command:", e);
                        close();
                    } finally {
                        buffer.release();
                    }

                    break;
                case Connected:
                    clientChannel.writeAndFlush(msg);
                    break;
                case Closed:
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            log.error("[" + vhost + "] ProxyBackendHandler [exceptionCaught] - msg: " + cause.getMessage(), cause);
            state = State.Failed;
            proxyConnection.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.warn("[{}] ProxyBackendHandler [channelInactive]", vhost);
            super.channelInactive(ctx);
            proxyConnection.close();
        }

        @Override
        public void receiveConnectionStart(short i, short i1, FieldTable fieldTable, byte[] bytes, byte[] bytes1) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionStart]");
            }
        }

        @Override
        public void receiveConnectionSecure(byte[] bytes) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionSecure]");
            }
        }

        @Override
        public void receiveConnectionRedirect(AMQShortString amqShortString, AMQShortString amqShortString1) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionRedirect]");
            }
        }

        @Override
        public void receiveConnectionTune(int i, long l, int i1) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionTune]");
            }
        }

        @Override
        public void receiveConnectionOpenOk(AMQShortString amqShortString) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionOpenOk]");
            }
            proxyConnection.writeFrame(connectResponseBody.generateFrame(0));
            state = State.Connected;
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [getProtocolVersion]");
            }
            return null;
        }

        @Override
        public ClientChannelMethodProcessor getChannelMethodProcessor(int i) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [getChannelMethodProcessor]");
            }
            return null;
        }

        @Override
        public void receiveConnectionClose(int replyCode, AMQShortString replyText,
                                           int classId, int methodId) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionClose]");
            }

            AMQFrame frame = new AMQFrame(0,
                    new ConnectionCloseBody(getProtocolVersion(), replyCode, replyText, classId, methodId));
            clientChannel.writeAndFlush(frame);
        }

        @Override
        public void receiveConnectionCloseOk() {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveConnectionCloseOk]");
            }
            close();
        }

        @Override
        public void receiveHeartbeat() {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveHeartbeat]");
            }
        }

        @Override
        public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [receiveProtocolHeader]");
            }
        }

        @Override
        public void setCurrentMethod(int i, int i1) {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [setCurrentMethod]");
            }
        }

        @Override
        public boolean ignoreAllButCloseOk() {
            if (log.isDebugEnabled()) {
                log.debug("ProxyBackendHandler [ignoreAllButCloseOk]");
            }
            return false;
        }
    }

    public void close() {
        state = State.Closed;
        this.brokerChannel.close();
    }

    enum State {
        Init,
        Connected,
        Failed,
        Closed
    }

}
