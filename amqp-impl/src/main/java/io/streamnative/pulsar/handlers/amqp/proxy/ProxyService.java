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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;

/**
 * This service is used for redirecting AMQP client request to proper AMQP protocol handler Broker.
 */
@Slf4j
public class ProxyService implements Closeable {

    @Getter
    private ProxyConfiguration proxyConfig;
    private String serviceUrl;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private PulsarClientImpl pulsarClient;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;
    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("amqp-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("amqp-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    private ZooKeeperClientFactory zkClientFactory = null;

    @Getter
    private Map<String, Pair<String, Integer>> vhostBrokerMap = Maps.newConcurrentMap();

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        checkNotNull(proxyConfig);
        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workerThreadFactory);
    }

    public void start() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new ServiceChannelInitializer(this, pulsarService));
        try {
            listenChannel = serverBootstrap.bind(proxyConfig.getServicePort().get()).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getServicePort().get(), e);
        }

        this.pulsarClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(proxyConfig.getBrokerServiceURL()).build();

        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle namespaceBundle) {
                log.info("onLoad namespaceBundle: {}", namespaceBundle);
                if (vhostBrokerMap.containsKey(namespaceBundle.getNamespaceObject().getLocalName())) {
                    log.info("onLoad vhostBrokerMap contain the namespaceBundle: {}", namespaceBundle);
                }
            }

            @Override
            public void unLoad(NamespaceBundle namespaceBundle) {
                log.info("unLoad namespaceBundle: {}", namespaceBundle);
                if (vhostBrokerMap.containsKey(namespaceBundle.getNamespaceObject().getLocalName())) {
                    log.info("unLoad vhostBrokerMap contain the namespaceBundle: {}", namespaceBundle);
                }
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                log.info("test namespaceBundle: {}", namespaceBundle);
                return true;
            }
        });
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
