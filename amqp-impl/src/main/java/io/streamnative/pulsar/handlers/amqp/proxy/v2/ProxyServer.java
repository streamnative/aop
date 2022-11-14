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
package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import io.streamnative.pulsar.handlers.amqp.AmqpProxyDirectHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyConfiguration;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

/**
 * Proxy server, the proxy server should be an individual service, it could be scale up.
 */
@Slf4j
public class ProxyServer {

    private final ProxyConfiguration config;
    private final PulsarService pulsar;
    private final Map<String, CompletableFuture<Producer<byte[]>>> producerMap;
    private PulsarServiceLookupHandler lookupHandler;

    public ProxyServer(ProxyConfiguration config, PulsarService pulsarService) {
        this.config = config;
        this.pulsar = pulsarService;
        this.producerMap = new ConcurrentHashMap<>();
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
                        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
                        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                                config.getAmqpExplicitFlushAfterFlushes(), true));
                        ch.pipeline().addLast("handler",
                                new ProxyClientConnection(config, lookupHandler, ProxyServer.this));
                        ch.pipeline().addLast("directHandler", new AmqpProxyDirectHandler());
                    }
                });
        bootstrap.bind(config.getAmqpProxyPort()).sync();
    }

    public CompletableFuture<Producer<byte[]>> getProducer(String topic) {
        PulsarClient client;
        try {
            client = pulsar.getClient();
        } catch (PulsarServerException e) {
            return CompletableFuture.failedFuture(e);
        }
        return producerMap.computeIfAbsent(topic, k -> {
            CompletableFuture<Producer<byte[]>> producerFuture = new CompletableFuture<>();
            client.newProducer()
                    .topic(topic)
                    .enableBatching(false)
                    .createAsync()
                    .thenAccept(producerFuture::complete)
                    .exceptionally(t -> {
                        producerFuture.completeExceptionally(t);
                        producerMap.remove(topic, producerFuture);
                        return null;
                    });
            return producerFuture;
        });
    }

}
