package io.streamnative.pulsar.handlers.amqp.redirect;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;

/**
 * This service is used for redirecting AMQP client request to proper AMQP protocol handler Broker.
 */
@Slf4j
public class RedirectService implements Closeable {

    @Getter
    private RedirectConfiguration redirectConfig;
    private String serviceUrl;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;
    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("amqp-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("amqp-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    @Getter
    private BrokerDiscoveryProvider brokerDiscoveryProvider;
    private ZooKeeperClientFactory zkClientFactory = null;

    @Getter
    private Map<String, String> vhostBrokerMap = Maps.newConcurrentMap();

    public RedirectService(RedirectConfiguration redirectConfig) {
        checkNotNull(redirectConfig);
        this.redirectConfig = redirectConfig;
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workerThreadFactory);
    }

    public void start() throws Exception {
        brokerDiscoveryProvider = new BrokerDiscoveryProvider(redirectConfig, getZooKeeperClientFactory());

        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new ServiceChannelInitializer(this));
        try {
            listenChannel = serverBootstrap.bind(redirectConfig.getServicePort().get()).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Failed to bind Pulsar Proxy on port " + redirectConfig.getServicePort().get(), e);
        }
    }

    public ZooKeeperClientFactory getZooKeeperClientFactory() {
        if (zkClientFactory == null) {
            zkClientFactory = new ZookeeperClientFactoryImpl();
        }
        // Return default factory
        return zkClientFactory;
    }

    @Override
    public void close() throws IOException {
        if (listenChannel != null) {
            listenChannel.close();
        }
    }
}
