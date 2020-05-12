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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;

/**
 * Maintains available active AMQP protocol handler broker list and returns next active broker
 * in round-robin for discovery service.
 */
@Slf4j
public class BrokerDiscoveryProvider implements Closeable {

    final ZookeeperCacheLoader localZkCache;
    private final AtomicInteger counter = new AtomicInteger();
    private PulsarClientImpl pulsarClient;
    private PulsarService pulsarService;

    BrokerDiscoveryProvider(ProxyConfiguration redirectConfig,
                            ZooKeeperClientFactory zkClientFactory) throws Exception {
        try {
            localZkCache = null;
//            localZkCache = new ZookeeperCacheLoader(zkClientFactory, redirectConfig.getZookeeperServers(),
//                    redirectConfig.getZookeeperSessionTimeoutMs());
        } catch (Exception e) {
            log.error("Failed to start zookeeper {}", e.getMessage(), e);
            throw new PulsarServerException("Failed to start zookeeper :" + e.getMessage(), e);
        }

        try {
            pulsarClient = (PulsarClientImpl) PulsarClient.builder()
                    .serviceUrl(redirectConfig.getBrokerServiceURL())
                    .build();
        } catch (Exception e) {
            log.error("Failed to init pulsarClient {}", e.getMessage(), e);
            throw new PulsarServerException("Failed to init pulsarClient :" + e.getMessage(), e);
        }
    }

    public LoadManagerReport nextBroker() throws PulsarServerException {
        List<LoadManagerReport> availableBrokers = localZkCache.getAvailableBrokers();
        if (availableBrokers.isEmpty()) {
            throw new PulsarServerException("No active broker is available.");
        } else {
            int brokersCount = availableBrokers.size();
            int nextIndex = signSafeMod(counter.getAndIncrement(), brokersCount);
            return availableBrokers.get(nextIndex);
        }
    }

    public static int signSafeMod(long dividend, int divisor) {
        int mod = (int)(dividend % (long)divisor);
        if (mod < 0) {
            mod += divisor;
        }
        return mod;
    }

    public Pair<InetSocketAddress, InetSocketAddress> lookupBroker(NamespaceName namespaceName)
            throws ExecutionException, InterruptedException, ProxyException {
        CompletableFuture<List<String>> completeFuture = this.pulsarClient.getLookup().
                getTopicsUnderNamespace(namespaceName, PulsarApi.CommandGetTopicsOfNamespace.Mode.ALL);
        List<String> topics = completeFuture.get();
        if (topics == null || topics.isEmpty()) {
            throw new ProxyException("The namespace has no topics.");
        }
        TopicName topicName = TopicName.get(topics.get(0));
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> completableFuture =
                this.pulsarClient.getLookup().getBroker(topicName);
        return completableFuture.get();
    }

    public void lookup(String namespaceName) throws Exception {
        NamespaceService namespaceService = pulsarService.getNamespaceService();
        Set<NamespaceBundle> namespaceBundleSet = namespaceService.getOwnedServiceUnits();
        NamespaceBundles namespaceBundles = namespaceService.getNamespaceBundleFactory().getBundles(NamespaceName.get(namespaceName));
    }

    @Override
    public void close() throws IOException {
        if (localZkCache != null) {
            localZkCache.close();
        }
    }

}
