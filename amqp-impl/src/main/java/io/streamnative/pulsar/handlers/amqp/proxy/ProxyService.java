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

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.netty.EventLoopUtil;

/**
 * This service is used for redirecting AMQP client request to proper AMQP protocol handler Broker.
 */
@Slf4j
public class ProxyService implements Closeable {

    @Getter
    private final ProxyConfiguration proxyConfig;
    @Getter
    private final PulsarService pulsarService;
    @Getter
    private LookupHandler lookupHandler;

    private Channel listenChannel;
    private final EventLoopGroup acceptorGroup;
    @Getter
    private final EventLoopGroup workerGroup;

    private static final int NUM_THREADS = Runtime.getRuntime().availableProcessors();

    private String tenant;

    public ProxyService(ProxyConfiguration proxyConfig, PulsarService pulsarService) {
        configValid(proxyConfig);

        this.proxyConfig = proxyConfig;
        this.pulsarService = pulsarService;
        this.tenant = this.proxyConfig.getAmqpTenant();
        this.acceptorGroup = EventLoopUtil.newEventLoopGroup(1, false,
                new DefaultThreadFactory("amqp-redirect-acceptor"));
        this.workerGroup = EventLoopUtil.newEventLoopGroup(NUM_THREADS * 2, false,
                new DefaultThreadFactory("amqp-redirect-io"));
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
