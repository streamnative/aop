package io.streamnative.pulsar.handlers.amqp.redirect;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpCommandDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public class RedirectHandler {

    private RedirectService redirectService;
    private RedirectConnection redirectConnection;
    private Channel clientChannel;
    @Getter
    private Channel brokerChannel;
    private State state;
    private List<Object> connectMsgList;

    RedirectHandler(RedirectService redirectService, RedirectConnection redirectConnection,
                    String amqpBrokerHost, int amqpBrokerPort, List<Object> connectMsgList) throws InterruptedException {
        this.redirectService = redirectService;
        this.redirectConnection = redirectConnection;
        clientChannel = this.redirectConnection.getCnx().channel();
        this.connectMsgList = connectMsgList;
        RedirectConfiguration redirectConfig = this.redirectService.getRedirectConfig();

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
                    break;
                case Connected:
                    clientChannel.writeAndFlush(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            log.error("RedirectBackendHandler [exceptionCaught] - msg: " + cause.getMessage(), cause);
        }
    }

    public void connected() {
        this.state = State.Connected;
    }

    enum State {
        Init,
        Connected
    }

}
