package io.streamnative.pulsar.handlers.amqp;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@Slf4j
public class ResourceContainer {

    public static PulsarService pulsarService;
    public static ZooKeeper zooKeeper;

    private final static Set<NamespaceName> namespaceSet = Sets.newConcurrentHashSet();
    private final static Map<NamespaceName, Set<AmqpConnection>> connectionMap = Maps.newConcurrentMap();

    public static void init(PulsarService pulsarService) {
        ResourceContainer.pulsarService = pulsarService;
        ResourceContainer.zooKeeper = pulsarService.getLocalZkCache().getZooKeeper();
    }

    public static void addConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        connectionMap.computeIfAbsent(namespaceName, ns -> {
            Set<AmqpConnection> connectionSet = connectionMap.get(ns);
            if (connectionSet == null) {
                connectionSet = Sets.newConcurrentHashSet();
            }
            connectionSet.add(amqpConnection);
            return connectionSet;
        });
        try {
            bundleDeleteListener(namespaceName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void bundleDeleteListener(NamespaceName namespaceName) throws KeeperException, InterruptedException {
        if (!namespaceSet.contains(namespaceName)) {
            namespaceSet.add(namespaceName);
            String path = "/namespace/" + namespaceName + "/0x00000000_0xffffffff";
            log.info("ResourceContainer [bundleDeleteListener] path: {}", path);
            zooKeeper.getData(path, watchedEvent -> {
                log.info("ResourceContainer [process] watchedEvent: {}", watchedEvent);
                if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeDeleted)) {
                    log.info("ResourceContainer nodeDeleted path: {}", watchedEvent.getPath());
                    namespaceSet.remove(namespaceName);

                    if (connectionMap.containsKey(namespaceName)) {
                        Set<AmqpConnection> connectionSet = connectionMap.get(namespaceName);
                        for (AmqpConnection connection : connectionSet) {
                            log.info("close connection: {}", connection);
                            if (connection.getOrderlyClose().compareAndSet(false, true)) {
                                connection.completeAndCloseAllChannels();
                                connection.getAmqpTopicManager().getExchangeTopics().clear();
                            }
                        }
                    }

                    if (ExchangeContainer.getExchangeMap().containsKey(namespaceName)) {
                        Map<String, AmqpExchange> exchangeMap = ExchangeContainer.getExchangeMap().get(namespaceName);
                        for (AmqpExchange exchange : exchangeMap.values()) {
                            if (exchange instanceof PersistentExchange) {
                                ((PersistentTopic) exchange.getTopic()).close();
                                try {
                                    ((PersistentExchange) exchange).getTopicCursorManager().close();
                                } catch (Exception e) {
                                    log.error("Failed to close topicCursorManager.", e);
                                }
                            }
                        }
                        exchangeMap.clear();
                        ExchangeContainer.getExchangeMap().remove(namespaceName);
                    }

                    if (QueueContainer.getQueueMap().containsKey(namespaceName)) {
                        Map<String, AmqpQueue> queueMap = QueueContainer.getQueueMap().get(namespaceName);
                        for (AmqpQueue queue : queueMap.values()) {
                            if (queue instanceof PersistentQueue) {
                                ((PersistentQueue) queue).getIndexTopic().close();
                            }
                        }
                    }

                }
            }, null);
        }
    }

}
