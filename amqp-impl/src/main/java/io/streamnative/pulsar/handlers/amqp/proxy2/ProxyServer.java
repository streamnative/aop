package io.streamnative.pulsar.handlers.amqp.proxy2;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;

/**
 * Proxy server, the proxy server should be an individual service, it could be scale up.
 */
@Slf4j
public class ProxyServer {

    private final ProxyConfiguration config;
    private final PulsarService pulsar;
    private PulsarServiceLookupHandler lookupHandler;

    public ProxyServer(ProxyConfiguration config, PulsarService pulsarService) {
        this.config = config;
        this.pulsar = pulsarService;
    }

    public void start() throws Exception {
        this.lookupHandler = new PulsarServiceLookupHandler(config, pulsar);
        // listen to the proxy port to receive amqp commands
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                                config.getAmqpExplicitFlushAfterFlushes(), true));
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("handler", new ProxyConnection(config, lookupHandler));
                    }
                });
        bootstrap.bind(config.getAmqpProxyPort()).sync();
    }

}
