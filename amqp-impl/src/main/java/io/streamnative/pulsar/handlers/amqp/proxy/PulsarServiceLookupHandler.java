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

import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;

/**
 * The proxy start with broker, use this lookup handler to find broker.
 */
@Slf4j
public class PulsarServiceLookupHandler implements LookupHandler, Closeable {

    private final PulsarClientImpl pulsarClient;
    private final MetadataStoreCacheLoader metadataStoreCacheLoader;

    public PulsarServiceLookupHandler(ProxyConfiguration proxyConfig, PulsarService pulsarService)
            throws Exception {
        this.pulsarClient = (PulsarClientImpl) pulsarService.getClient();
        this.metadataStoreCacheLoader = new MetadataStoreCacheLoader(pulsarService.getPulsarResources(),
                proxyConfig.getBrokerLookupTimeoutSeconds());
    }

    @Override
    public CompletableFuture<Pair<String, Integer>> findBroker(TopicName topicName,
                                                              String protocolHandlerName) throws Exception  {
        CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();

        // lookup the broker for the given topic
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookup =
                pulsarClient.getLookup().getBroker(topicName);
        lookup.whenComplete((result, throwable) -> {
            if (throwable != null) {
                lookupResult.completeExceptionally(throwable);
                return;
            }
            if (result == null || result.getLeft() == null) {
                lookupResult.completeExceptionally(new ProxyException(
                        "Unable to resolve the broker for the topic: " + topicName));
                return;
            }
            String hostAndPort = result.getLeft().toString();

            // fetch the protocol handler data
            List<LoadManagerReport> brokers = metadataStoreCacheLoader.getAvailableBrokers();
            Optional<LoadManagerReport> serviceLookupData =
                    brokers.stream().filter(b -> matches(hostAndPort, b)).findAny();
            if (!serviceLookupData.isPresent()) {
                lookupResult.completeExceptionally(new ProxyException(
                        "Unable to locate metadata for the broker of the topic: " + topicName));
                return;
            }

            Optional<String> protocolData = serviceLookupData.get().getProtocol(protocolHandlerName);
            if (!protocolData.isPresent()) {
                lookupResult.completeExceptionally(new ProxyException(
                        "No protocol data is available for the broker of the topic: " + topicName));
                return;
            }

            String amqpBrokerAddress = protocolData.get();
            if (!StringUtils.startsWith(amqpBrokerAddress, AmqpProtocolHandler.PLAINTEXT_PREFIX)
                && !StringUtils.startsWith(amqpBrokerAddress, AmqpProtocolHandler.SSL_PREFIX)) {
                amqpBrokerAddress = AmqpProtocolHandler.PLAINTEXT_PREFIX + amqpBrokerAddress;
            }
            URI amqpBrokerUri;
            try {
                amqpBrokerUri = new URI(amqpBrokerAddress);
            } catch (URISyntaxException e) {
                lookupResult.completeExceptionally(e);
                return;
            }
            lookupResult.complete(Pair.of(amqpBrokerUri.getHost(), amqpBrokerUri.getPort()));
        });

        return lookupResult;
    }

    private static boolean matches(String hostAndPort, LoadManagerReport serviceData) {
        return StringUtils.contains(serviceData.getPulsarServiceUrl(), hostAndPort)
            || StringUtils.contains(serviceData.getPulsarServiceUrlTls(), hostAndPort);
    }

    @Override
    public void close() throws IOException {
        this.metadataStoreCacheLoader.close();
    }
}
