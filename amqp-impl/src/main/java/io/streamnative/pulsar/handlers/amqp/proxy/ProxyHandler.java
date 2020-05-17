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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
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
        clientChannel = this.proxyConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;
        this.vhost = vhost;

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("processor", new ProxyBackendHandler(responseBody));
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(amqpBrokerHost, amqpBrokerPort);
        brokerChannel = channelFuture.channel();
        channelFuture.addListener(future -> {
            if (!future.isSuccess()) {
                // Close the connection if the connection attempt has failed.
                clientChannel.close();
            }
        });
        state = State.Init;
        log.info("Broker channel connect. vhost: {}, broker: {}:{}, isOpen: {}",
                vhost, amqpBrokerHost, amqpBrokerPort, brokerChannel.isOpen());
    }

    private class ProxyBackendHandler extends ChannelInboundHandlerAdapter implements
            ClientMethodProcessor<ClientChannelMethodProcessor>, FutureListener<Void> {

        private ChannelHandlerContext cnx;
        private AMQMethodBody connectResponseBody;
        private AMQClientDecoder clientDecoder;

        ProxyBackendHandler(AMQMethodBody responseBody) {
            this.connectResponseBody = responseBody;
            clientDecoder = new AMQClientDecoder(this);
        }

        @Override
        public void operationComplete(Future future) throws Exception {
            // This is invoked when the write operation on the paired connection
            // is completed
            if (future.isSuccess()) {
                brokerChannel.read();
            } else {
                log.warn("[{}] [{}] Failed to write on proxy connection. Closing both connections.", clientChannel,
                        brokerChannel, future.cause());
                clientChannel.close();
            }

        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("[{}] ProxyBackendHandler [channelActive]", vhost);
            this.cnx = ctx;
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                ((ByteBuf) msg).retain();
                brokerChannel.writeAndFlush(msg).syncUninterruptibly();
                ((ByteBuf) msg).release();
            }
            brokerChannel.read();
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            log.info("[{}] ProxyBackendHandler [channelRead]", vhost);
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
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            log.error("[" + vhost + "] ProxyBackendHandler [channelInactive]");
            super.channelInactive(ctx);
        }

        @Override
        public void receiveConnectionStart(short i, short i1, FieldTable fieldTable, byte[] bytes, byte[] bytes1) {
            log.info("ProxyBackendHandler [receiveConnectionStart]");
        }

        @Override
        public void receiveConnectionSecure(byte[] bytes) {
            log.info("ProxyBackendHandler [receiveConnectionSecure]");
        }

        @Override
        public void receiveConnectionRedirect(AMQShortString amqShortString, AMQShortString amqShortString1) {
            log.info("ProxyBackendHandler [receiveConnectionRedirect]");
        }

        @Override
        public void receiveConnectionTune(int i, long l, int i1) {
            log.info("ProxyBackendHandler [receiveConnectionTune]");
        }

        @Override
        public void receiveConnectionOpenOk(AMQShortString amqShortString) {
            log.info("ProxyBackendHandler [receiveConnectionOpenOk]");
            proxyConnection.writeFrame(connectResponseBody.generateFrame(0));
            state = State.Connected;
        }

        @Override
        public ProtocolVersion getProtocolVersion() {
            log.info("ProxyBackendHandler [getProtocolVersion]");
            return null;
        }

        @Override
        public ClientChannelMethodProcessor getChannelMethodProcessor(int i) {
            log.info("ProxyBackendHandler [getChannelMethodProcessor]");
            return null;
        }

        @Override
        public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {
            log.info("ProxyBackendHandler [receiveConnectionClose]");
        }

        @Override
        public void receiveConnectionCloseOk() {
            log.info("ProxyBackendHandler [receiveConnectionCloseOk]");
        }

        @Override
        public void receiveHeartbeat() {
            log.info("ProxyBackendHandler [receiveHeartbeat]");
        }

        @Override
        public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
            log.info("ProxyBackendHandler [receiveProtocolHeader]");
        }

        @Override
        public void setCurrentMethod(int i, int i1) {
            log.info("ProxyBackendHandler [setCurrentMethod]");
        }

        @Override
        public boolean ignoreAllButCloseOk() {
            log.info("ProxyBackendHandler [ignoreAllButCloseOk]");
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
