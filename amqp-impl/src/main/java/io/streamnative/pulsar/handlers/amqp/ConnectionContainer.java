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
import org.apache.zookeeper.ZooKeeper;

/**
 * Connection container, listen bundle unload event, release connection resource.
 */
@Slf4j
public class ConnectionContainer {

    private static PulsarService pulsarService;
    public static ZooKeeper zooKeeper;
    private static Map<NamespaceName, Set<AmqpConnection>> connectionMap = Maps.newConcurrentMap();

    public static void init(PulsarService pulsarService) {
        ConnectionContainer.pulsarService = pulsarService;
        ConnectionContainer.zooKeeper = pulsarService.getLocalZkCache().getZooKeeper();
        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle namespaceBundle) {
                log.info("ConnectionContainer [onLoad] namespaceBundle: {}", namespaceBundle);
                ExchangeContainer.initBuildInExchange(namespaceBundle.getNamespaceObject());
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

                if (ExchangeContainer.getExchangeMap().containsKey(namespaceName)) {
                    ExchangeContainer.getExchangeMap().get(namespaceName).clear();
                    ExchangeContainer.getExchangeMap().remove(namespaceName);
                }

                if (QueueContainer.getQueueMap().containsKey(namespaceName)) {
                    QueueContainer.getQueueMap().get(namespaceName).clear();
                    QueueContainer.getQueueMap().remove(namespaceName);
                }
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return true;
            }
        });
    }

    public static void addConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        connectionMap.compute(namespaceName, (ns, connectionSet) -> {
            if (connectionSet == null) {
                connectionSet = Sets.newConcurrentHashSet();
            }
            connectionSet.add(amqpConnection);
            return connectionSet;
        });
    }

    public static void removeConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        connectionMap.getOrDefault(namespaceName, Collections.emptySet()).remove(amqpConnection);
    }

}
