package io.streamnative.pulsar.handlers.amqp.proxy2;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;

import java.util.concurrent.CompletableFuture;

/**
 * This class is used to transfer data between proxy and broker.
 */
@Slf4j
public class ProxyBrokerConnection extends ChannelInboundHandlerAdapter {

    private ChannelHandlerContext ctx;
    private Channel channel;
    private final Channel clientChannel;

    public ProxyBrokerConnection(String host, Integer port, Channel clientChannel) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                                1000, true));
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("processor", this);
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
        this.clientChannel = clientChannel;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isInfoEnabled()) {
            log.info("xxxx [ProxyBrokerConn] writeFrame: " + frame);
        }
        this.ctx.writeAndFlush(frame);
    }

}
