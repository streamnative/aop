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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Proxy handler is the bridge between client and broker.
 */
@Slf4j
public class ProxyHandler {

    private ProxyService proxyService;
    private ProxyConnection proxyConnection;
    private Channel clientChannel;
    @Getter
    private Channel brokerChannel;
    private State state;
    private List<Object> connectMsgList;

    ProxyHandler(ProxyService proxyService, ProxyConnection proxyConnection,
                 String amqpBrokerHost, int amqpBrokerPort, List<Object> connectMsgList) throws InterruptedException {
        this.proxyService = proxyService;
        this.proxyConnection = proxyConnection;
        clientChannel = this.proxyConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientChannel.eventLoop())
                .channel(clientChannel.getClass())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("processor", new RedirectBackendHandler());
                    }
                });
        ChannelFuture channelFuture = bootstrap.connect(amqpBrokerHost, amqpBrokerPort);
        brokerChannel = channelFuture.channel();
        state = State.Init;
        log.info("broker channel connect isOpen: {}", brokerChannel.isOpen());
    }

    private class RedirectBackendHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            log.info("RedirectBackendHandler [channelActive]");
            super.channelActive(ctx);
            for (Object msg : connectMsgList) {
                brokerChannel.writeAndFlush(msg);
            }
            state = State.Connected;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            log.info("RedirectBackendHandler [channelRead]");
            switch (state) {
                case Init:
                case Failed:
                    break;
                case Connected:
                    clientChannel.writeAndFlush(msg);
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            log.error("RedirectBackendHandler [exceptionCaught] - msg: " + cause.getMessage(), cause);
            state = State.Failed;
        }
    }

    enum State {
        Init,
        Connected,
        Failed
    }

}
