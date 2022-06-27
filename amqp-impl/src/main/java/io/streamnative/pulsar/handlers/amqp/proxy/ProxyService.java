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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.Maps;
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
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * This service is used for redirecting AMQP client request to proper AMQP protocol handler Broker.
 */
@Slf4j
public class ProxyService implements Closeable {

    @Getter
    private ProxyConfiguration proxyConfig;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private LookupHandler lookupHandler;

    private Channel listenChannel;
    private EventLoopGroup acceptorGroup;
    @Getter
    private EventLoopGroup workerGroup;

    private DefaultThreadFactory acceptorThreadFactory = new DefaultThreadFactory("amqp-redirect-acceptor");
    private DefaultThreadFactory workerThreadFactory = new DefaultThreadFactory("amqp-redirect-io");
    private static final int numThreads = Runtime.getRuntime().availableProcessors();

    @Getter
    private static final Map<String, Set<ProxyConnection>> vhostConnectionMap = Maps.newConcurrentMap();

    private String tenant;

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        configValid(proxyConfig);

        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        this.tenant = this.proxyConfig.getAmqpTenant();
        acceptorGroup = EventLoopUtil.newEventLoopGroup(1, false, acceptorThreadFactory);
        workerGroup = EventLoopUtil.newEventLoopGroup(numThreads, false, workerThreadFactory);
    }

    private void configValid(ProxyConfiguration proxyConfig) {
        checkNotNull(proxyConfig);
        checkArgument(proxyConfig.getAmqpProxyPort() > 0);
        checkNotNull(proxyConfig.getAmqpTenant());
        checkNotNull(proxyConfig.getBrokerServiceURL());
    }

    public void start() throws Exception {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(workerGroup));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.childHandler(new ServiceChannelInitializer(this));
        try {
            listenChannel = serverBootstrap.bind(proxyConfig.getAmqpProxyPort()).sync().channel();
        } catch (InterruptedException e) {
            throw new IOException("Failed to bind Pulsar Proxy on port " + proxyConfig.getAmqpProxyPort(), e);
        }

        this.lookupHandler = new PulsarServiceLookupHandler(proxyConfig, pulsarService);
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

    @Override
    public void close() throws IOException {
        if (lookupHandler != null) {
            lookupHandler.close();
        }
        if (listenChannel != null) {
            listenChannel.close();
        }
    }
}
