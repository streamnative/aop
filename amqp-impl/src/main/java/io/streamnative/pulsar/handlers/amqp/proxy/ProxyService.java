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

import com.google.common.collect.BoundType;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

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
    @Getter
    private LookupHandler lookupHandler;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;
    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("amqp-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("amqp-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    private ZooKeeperClientFactory zkClientFactory = null;

    @Getter
    private static final Map<String, Pair<String, Integer>> vhostBrokerMap = Maps.newConcurrentMap();
    @Getter
    private static final Map<String, Set<ProxyConnection>> vhostConnectionMap = Maps.newConcurrentMap();

    private String tenant;

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        configValid(proxyConfig);

        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        this.tenant = this.proxyConfig.getAmqpTenant();
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, workerThreadFactory);
    }

    private void configValid(ProxyConfiguration proxyConfig) {
        checkNotNull(proxyConfig);
        checkNotNull(proxyConfig.getProxyPort());
        checkNotNull(proxyConfig.getBrokerServiceURL());
    }

    public void start() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new ServiceChannelInitializer(this));
        try {
            listenChannel = serverBootstrap.bind(proxyConfig.getProxyPort().get()).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getProxyPort().get(), e);
        }

        this.pulsarClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(
                proxyConfig.getBrokerServiceURL()).build();

        this.lookupHandler = new PulsarServiceLookupHandler(pulsarService, pulsarClient);
    }

    private void releaseConnection(String namespaceName) {
        log.info("release connection");
        if (vhostConnectionMap.containsKey(namespaceName)) {
            Set<ProxyConnection> proxyConnectionSet = vhostConnectionMap.get(namespaceName);
            for (ProxyConnection proxyConnection : proxyConnectionSet) {
                proxyConnection.close();
            }
        }
    }

    public void cacheVhostMap(String vhost, Pair<String, Integer> lookupData) {
        this.vhostBrokerMap.put(vhost, lookupData);
        try {
            NamespaceBundle namespaceBundle = new NamespaceBundle(
                    NamespaceName.get(tenant, vhost),
                    Range.range(NamespaceBundles.FULL_LOWER_BOUND, BoundType.CLOSED,
                            NamespaceBundles.FULL_UPPER_BOUND, BoundType.CLOSED),
                    pulsarService.getNamespaceService().getNamespaceBundleFactory());
            String path = "/namespace/" + namespaceBundle.toString();

            getPulsarService().getLocalZkCache().getZooKeeper().getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    log.info("process watchedEvent: {}", watchedEvent);
                    synchronized (vhostBrokerMap) {
                        String path = watchedEvent.getPath();
                        if (watchedEvent.getType().equals(Event.EventType.NodeDeleted)) {
                            String[] stringSplits = path.split("/");
                            String vhost = stringSplits[stringSplits.length - 2];
                            if (vhostBrokerMap.containsKey(vhost)) {
                                log.info("unLoad vhostBrokerMap contain the namespaceBundle: {}", path);
                                vhostBrokerMap.remove(vhost);
                                releaseConnection(vhost);
                            }
                        }
                    }
                }
            }, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
                    try {
                        if (bytes != null) {
                            NamespaceEphemeralData ephemeralData =
                                    ObjectMapperFactory.getThreadLocal().readValue(bytes, NamespaceEphemeralData.class);
                            if (log.isDebugEnabled()) {
                                log.debug("processResult ephemeralData: {}", ephemeralData);
                            }
                        } else {
                            if (log.isDebugEnabled()) {
                                log.debug("processResult ephemeralData is null");
                            }
                        }
                    } catch (IOException e) {
                        log.error("Read ephemeralData failed", e);
                    }
                }
            }, null);
        } catch (Exception e) {
            log.error("Add watcher failed. vhost: {}, lookupData: {}", vhost, lookupData, e);
        }
    }

    @Override
    public void close() throws IOException {
        if (listenChannel != null) {
            listenChannel.close();
        }
    }
}
