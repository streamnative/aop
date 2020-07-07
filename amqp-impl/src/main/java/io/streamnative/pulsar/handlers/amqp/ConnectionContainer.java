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
package io.streamnative.pulsar.handlers.amqp;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Connection container, listen bundle unload event, release connection resource.
 */
@Slf4j
public class ConnectionContainer {

    private Map<NamespaceName, Set<AmqpConnection>> connectionMap = Maps.newConcurrentMap();

    protected ConnectionContainer(PulsarService pulsarService,
                                  ExchangeContainer exchangeContainer, QueueContainer queueContainer) {
        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle namespaceBundle) {
                int brokerPort = pulsarService.getBrokerListenPort().isPresent()
                        ? pulsarService.getBrokerListenPort().get() : 0;
                log.info("ConnectionContainer [onLoad] namespaceBundle: {}, brokerPort: {}",
                        namespaceBundle, brokerPort);
            }

            @Override
            public void unLoad(NamespaceBundle namespaceBundle) {
                log.info("ConnectionContainer [unLoad] namespaceBundle: {}", namespaceBundle);
                NamespaceName namespaceName = namespaceBundle.getNamespaceObject();

                if (connectionMap.containsKey(namespaceName)) {
                    Set<AmqpConnection> connectionSet = connectionMap.get(namespaceName);
                    for (AmqpConnection connection : connectionSet) {
                        log.info("close connection: {}", connection);
                        if (connection.getOrderlyClose().compareAndSet(false, true)) {
                            connection.completeAndCloseAllChannels();
                            connection.close();
                        }
                    }
                    connectionSet.clear();
                    connectionMap.remove(namespaceName);
                }

                if (exchangeContainer.getExchangeMap().containsKey(namespaceName)) {
                    exchangeContainer.getExchangeMap().remove(namespaceName);
                }

                if (queueContainer.getQueueMap().containsKey(namespaceName)) {
                    queueContainer.getQueueMap().remove(namespaceName);
                }
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return true;
            }
        });
    }

    public void addConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        connectionMap.compute(namespaceName, (ns, connectionSet) -> {
            if (connectionSet == null) {
                connectionSet = Sets.newConcurrentHashSet();
            }
            connectionSet.add(amqpConnection);
            return connectionSet;
        });
    }

    public void removeConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        if (namespaceName == null) {
            return;
        }
        connectionMap.getOrDefault(namespaceName, Collections.emptySet()).remove(amqpConnection);
    }

}
