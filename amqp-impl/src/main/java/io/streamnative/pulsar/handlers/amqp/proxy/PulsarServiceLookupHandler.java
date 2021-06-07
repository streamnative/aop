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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

/**
 * The proxy start with broker, use this lookup handler to find broker.
 */
@Slf4j
public class PulsarServiceLookupHandler implements LookupHandler {

    private PulsarService pulsarService;

    private PulsarClientImpl pulsarClient;

    private MetadataCache<LocalBrokerData> serviceLookupDataCache;

    public PulsarServiceLookupHandler(PulsarService pulsarService, PulsarClientImpl pulsarClient) {
        this.pulsarService = pulsarService;
        this.pulsarClient = pulsarClient;
        this.serviceLookupDataCache = pulsarService.getLocalMetadataStore().getMetadataCache(LocalBrokerData.class);
    }

    @Override
    public CompletableFuture<Pair<String, Integer>> findBroker(TopicName topicName,
                                                               String protocolHandlerName) throws Exception {
        CompletableFuture<Pair<String, Integer>> lookupResult = new CompletableFuture<>();
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> lookup =
                pulsarClient.getLookup().getBroker(topicName);

        lookup.whenComplete((pair, throwable) -> {
            final String hostName = pair.getLeft().getHostName();
            List<String> webServiceList;
            try {
                webServiceList = pulsarService.getZkClient().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT, null);
            } catch (Exception e) {
                log.error("Failed to get children of the path {}", LoadManager.LOADBALANCE_BROKERS_ROOT, e);
                lookupResult.completeExceptionally(e);
                return;
            }

            String hostAndPort = pair.getLeft().getHostName() + ":" + pair.getLeft().getPort();
            List<String> matchWebUri = new ArrayList<>();
            for (String webService : webServiceList) {
                if (webService.startsWith(hostName)) {
                    matchWebUri.add(webService);
                }
            }

            List<CompletableFuture<Optional<LocalBrokerData>>> futureList = new ArrayList<>();
            for (String webService : matchWebUri) {
                String path = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + webService;
                futureList.add(serviceLookupDataCache.get(path));
                serviceLookupDataCache.get(path).whenComplete((lookupDataOptional, serviceLookupThrowable) -> {
                    if (serviceLookupThrowable != null) {
                        log.error("Failed to get service lookup data from path {}", path, serviceLookupThrowable);
                        lookupResult.completeExceptionally(serviceLookupThrowable);
                        return;
                    }
                    if (!lookupDataOptional.isPresent()) {
                        log.error("Service lookup data is null path {}", path);
                        lookupResult.completeExceptionally(
                                new ProxyException("Service lookup data is null path " + path));
                        return;
                    }
                    ServiceLookupData serviceLookupData = lookupDataOptional.get();
                    if (serviceLookupData.getPulsarServiceUrl().contains("" + pair.getLeft().getPort())) {
                        if (serviceLookupData.getProtocol(protocolHandlerName).isPresent()) {
                            String amqpBrokerUrl = serviceLookupData.getProtocol(protocolHandlerName).get();
                            String[] splits = amqpBrokerUrl.split(":");
                            String port = splits[splits.length - 1];
                            int amqpBrokerPort = Integer.parseInt(port);
                            lookupResult.complete(Pair.of(hostName, amqpBrokerPort));
                        }
                    }
                });
            }

            FutureUtil.waitForAll(futureList).whenComplete((ignored, t) -> {
                if (t != null) {
                    log.error("Failed to get service lookup data.", t);
                    lookupResult.completeExceptionally(t);
                    return;
                }
                boolean match = false;
                for (CompletableFuture<Optional<LocalBrokerData>> future : futureList) {
                    Optional<LocalBrokerData> optionalServiceLookupData = future.join();
                    if (!optionalServiceLookupData.isPresent()) {
                        log.warn("Service lookup data is null.");
                        continue;
                    }
                    ServiceLookupData data = optionalServiceLookupData.get();
                    if (log.isDebugEnabled()) {
                        log.debug("Handle service lookup data for {}, pulsarUrl: {}, pulsarUrlTls: "
                                        + "{}, webUrl: {}, webUrlTls: {} kafka: {}",
                                topicName, data.getPulsarServiceUrl(), data.getPulsarServiceUrlTls(),
                                data.getWebServiceUrl(), data.getWebServiceUrlTls(),
                                data.getProtocol(AmqpProtocolHandler.PROTOCOL_NAME));
                    }
                    if (!lookupDataContainsAddress(data, hostAndPort)) {
                        continue;
                    }
                    if (!data.getProtocol(protocolHandlerName).isPresent()) {
                        log.warn("Protocol data is null.");
                        continue;
                    }
                    String amqpBrokerUrl = data.getProtocol(protocolHandlerName).get();
                    String[] splits = amqpBrokerUrl.split(":");
                    String port = splits[splits.length - 1];
                    int amqpBrokerPort = Integer.parseInt(port);
                    lookupResult.complete(Pair.of(hostName, amqpBrokerPort));
                    match = true;
                }

                if (!match) {
                    // no matching lookup data in all matchBrokers.
                    lookupResult.completeExceptionally(new ProxyException(
                            String.format("Not able to search %s in all child of zk://loadbalance", pair.getLeft())));
                }
            });

        });

        return lookupResult;
    }

    private static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return (data.getPulsarServiceUrl() != null && data.getPulsarServiceUrl().contains(hostAndPort))
                || (data.getPulsarServiceUrlTls() != null && data.getPulsarServiceUrlTls().contains(hostAndPort))
                || (data.getWebServiceUrl() != null && data.getWebServiceUrl().contains(hostAndPort))
                || (data.getWebServiceUrlTls() != null && data.getWebServiceUrlTls().contains(hostAndPort));
    }

}
