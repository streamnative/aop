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

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentMessageExpiryMonitor;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.protocol.Commands;

/**
 * Container for all queues in the broker.
 */
@Slf4j
public class QueueContainer {

    private AmqpTopicManager amqpTopicManager;
    private PulsarService pulsarService;
    private ExchangeContainer exchangeContainer;
    private AmqpServiceConfiguration config;
    private ScheduledExecutorService executorService;
    private static final AtomicIntegerFieldUpdater<PersistentMessageExpiryMonitor> expirationCheckInProgressUpdater;

    static {
        try {
            Field field = PersistentMessageExpiryMonitor.class.getDeclaredField("expirationCheckInProgressUpdater");
            field.setAccessible(true);
            expirationCheckInProgressUpdater =
                    (AtomicIntegerFieldUpdater<PersistentMessageExpiryMonitor>) field.get(null);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    protected QueueContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService,
                             ExchangeContainer exchangeContainer, AmqpServiceConfiguration config) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
        this.exchangeContainer = exchangeContainer;
        this.config = config;
        this.executorService = Executors.newScheduledThreadPool(1, new DefaultThreadFactory("expireMessages-checker"));
        schedule();
    }

    private void schedule() {
        executorService.schedule(this::findExpireMessages, 500, TimeUnit.MILLISECONDS);
    }

    @Getter
    private Map<NamespaceName, Map<String, CompletableFuture<AmqpQueue>>> queueMap = new ConcurrentHashMap<>();

    private void findExpireMessages() {
        try {
            queueMap.values().stream().flatMap(m -> m.values().stream()).forEach(future -> {
                if (future.isDone()) {
                    AmqpQueue amqpQueue = null;
                    try {
                        amqpQueue = future.get();
                    } catch (Exception e) {
                        log.error("", e);
                    }
                    if (amqpQueue instanceof PersistentQueue persistentQueue) {
                        Topic topic = persistentQueue.getTopic();
                        Subscription amqpDefault = topic.getSubscription("AMQP_DEFAULT");
                        // Create a default subscription for the queue.
                        if (amqpDefault == null) {
                            try {
                                pulsarService.getClient()
                                        .newConsumer()
                                        .topic(topic.getName())
                                        .subscriptionType(SubscriptionType.Shared)
                                        .subscriptionName("AMQP_DEFAULT")
                                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                                        .consumerName(UUID.randomUUID().toString())
                                        .receiverQueueSize(1000)
                                        .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
                                        .subscribe()
                                        .close();
                            } catch (Exception e) {
                                log.error("topic [{}] subscription create fail", topic.getName());
                            }
                            return;
                        }
                        if (amqpDefault instanceof PersistentSubscription subscription) {
                            PersistentMessageExpiryMonitor expiryMonitor = subscription.getExpiryMonitor();
                            ManagedCursor cursor = subscription.getCursor();
                            if ((subscription.getNumberOfEntriesInBacklog(false) == 0) || (
                                    subscription.getDispatcher() != null && subscription.getDispatcher()
                                            .isConsumerConnected())) {
                                return;
                            }
                            expireMessages(persistentQueue, expiryMonitor, cursor);
                        }
                    }
                }
            });
        } catch (Exception e) {
            log.error("expire message check fail", e);
        } finally {
            schedule();
        }
    }

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    public void expireMessages(PersistentQueue persistentQueue, PersistentMessageExpiryMonitor expiryMonitor,
                               ManagedCursor cursor) {
        Map<String, Object> properties = persistentQueue.getArguments();
        if (expirationCheckInProgressUpdater.compareAndSet(expiryMonitor, FALSE, TRUE)) {
            Object queueTtl = properties.get("x-message-ttl");
            Object routingKey = properties.get("x-dead-letter-routing-key");
            cursor.asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchActiveEntries, entry -> {
                try {
                    MessageMetadata messageMetadata = Commands.parseMessageMetadata(entry.getDataBuffer());
                    // queue ttl
                    int expireTime = -1;
                    if (queueTtl != null) {
                        try {
                            expireTime = Integer.parseInt(queueTtl.toString());
                        } catch (NumberFormatException ignored) {
                        }
                    }
                    // message ttl
                    KeyValue keyValue = messageMetadata.getPropertiesList().stream()
                            .filter(kv -> MessageConvertUtils.PROP_EXPIRATION.equals(kv.getKey()))
                            .findFirst()
                            .orElse(null);
                    int value;
                    if (keyValue != null && (value = Integer.parseInt(keyValue.getValue())) > 0) {
                        expireTime = expireTime > 0 ? Math.min(expireTime, value) : value;
                    }
                    if (expireTime <= 0) {
                        return false;
                    }
                    boolean expired = isEntryExpired(expireTime, messageMetadata.getPublishTime());
                    if (expired) {
                        if (persistentQueue.getProducer() != null && StringUtils.isNotBlank(routingKey.toString())) {
                            messageMetadata.getPropertiesList()
                                    .forEach(kv -> {
                                        switch (kv.getKey()) {
                                            // reset routing_key
                                            case MessageConvertUtils.PROP_ROUTING_KEY ->
                                                    kv.setValue(routingKey.toString());
                                            // reset expiration
                                            case MessageConvertUtils.PROP_EXPIRATION -> kv.setValue("0");
                                            // reset exchange
                                            case MessageConvertUtils.PROP_EXCHANGE -> kv.setValue(
                                                    persistentQueue.getDleExchangeName());
                                            default -> {
                                            }
                                        }
                                    });
                            MessageImpl<byte[]> message = MessageImpl.create(null, null,
                                    messageMetadata,
                                    entry.getDataBuffer(),
                                    Optional.empty(), null, Schema.BYTES, 0, true, -1L);
                            persistentQueue.getProducer()
                                    .sendAsync(message)
                                    .whenComplete((messageId, throwable) -> {
                                        if (throwable != null) {
                                            log.error("dead-letter queue [{}] send fail",
                                                    persistentQueue.getProducer().getTopic(), throwable);
                                        }
                                    });
                        } else {
                            log.warn("Message expired, no dead-letter-producer, [{}] message auto ack", persistentQueue.getTopic());
                        }
                    }
                    return expired;
                } catch (Exception e) {
                    log.error("[{}][{}] Error deserializing message for expiry check",
                            cursor.getManagedLedger().getName(), cursor.getName(), e);
                } finally {
                    entry.release();
                }
                return false;
            }, expiryMonitor, null);
        }
    }

    public static boolean isEntryExpired(int expireMillis, long entryTimestamp) {
        return System.currentTimeMillis() > entryTimestamp + expireMillis;
    }

    /**
     * Get or create queue.
     *
     * @param namespaceName   namespace in pulsar
     * @param queueName       name of queue
     * @param createIfMissing true to create the queue if not existed
     *                        false to get the queue and return null if not existed
     * @return the completableFuture of get result
     */
    public CompletableFuture<AmqpQueue> asyncGetQueue(NamespaceName namespaceName, String queueName,
                                                      boolean createIfMissing) {
        CompletableFuture<AmqpQueue> queueCompletableFuture = new CompletableFuture<>();
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            log.error("[{}][{}] Parameter error, namespaceName or queueName is empty.", namespaceName, queueName);
            queueCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("NamespaceName or queueName is empty"));
            return queueCompletableFuture;
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            queueCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
            return queueCompletableFuture;
        }
        queueMap.putIfAbsent(namespaceName, new ConcurrentHashMap<>());
        CompletableFuture<AmqpQueue> existingAmqpExchangeFuture = queueMap.get(namespaceName).
                putIfAbsent(queueName, queueCompletableFuture);
        if (existingAmqpExchangeFuture != null) {
            return existingAmqpExchangeFuture;
        } else {
            String topicName = PersistentQueue.getQueueTopicName(namespaceName, queueName);
            CompletableFuture<Topic> topicCompletableFuture =
                    amqpTopicManager.getTopic(topicName, createIfMissing, null);
            topicCompletableFuture.whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    log.error("[{}][{}] Failed to get queue topic.", namespaceName, queueName, throwable);
                    queueCompletableFuture.completeExceptionally(throwable);
                    removeQueueFuture(namespaceName, queueName);
                } else {
                    if (null == topic) {
                        log.warn("[{}][{}] Queue topic did not exist.", namespaceName, queueName);
                        queueCompletableFuture.complete(null);
                        removeQueueFuture(namespaceName, queueName);
                    } else {
                        // recover metadata if existed
                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                        Map<String, String> properties = persistentTopic.getManagedLedger().getProperties();

                        // TODO: reset connectionId, exclusive and autoDelete
                        PersistentQueue amqpQueue = new PersistentQueue(queueName, persistentTopic,
                                0, false, false, properties);
                        if (!config.isAmqpMultiBundleEnable()) {
                            try {
                                amqpQueue.recoverRoutersFromQueueProperties(properties, exchangeContainer,
                                        namespaceName);
                            } catch (Exception e) {
                                log.error("[{}][{}] Failed to recover routers for queue from properties.",
                                        namespaceName, queueName, e);
                                queueCompletableFuture.completeExceptionally(e);
                                removeQueueFuture(namespaceName, queueName);
                                return;
                            }
                        }
                        queueCompletableFuture.complete(amqpQueue);
                    }
                }
            });
        }
        return queueCompletableFuture;
    }

    // TODO need to improve
    public CompletableFuture<AmqpQueue> asyncGetQueue(NamespaceName namespaceName, String queueName,
                                                      boolean passive,
                                                      boolean durable,
                                                      boolean exclusive,
                                                      boolean autoDelete,
                                                      boolean nowait,
                                                      Map<String, Object> arguments) {
        CompletableFuture<AmqpQueue> queueCompletableFuture = new CompletableFuture<>();
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            log.error("[{}][{}] Parameter error, namespaceName or queueName is empty.", namespaceName, queueName);
            queueCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("NamespaceName or queueName is empty"));
            return queueCompletableFuture;
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            queueCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
            return queueCompletableFuture;
        }
        queueMap.putIfAbsent(namespaceName, new ConcurrentHashMap<>());
        CompletableFuture<AmqpQueue> existingAmqpExchangeFuture = queueMap.get(namespaceName).
                putIfAbsent(queueName, queueCompletableFuture);
        if (existingAmqpExchangeFuture != null) {
            return existingAmqpExchangeFuture;
        } else {
            String topicName = PersistentQueue.getQueueTopicName(namespaceName, queueName);

            Map<String, String> initProperties = new HashMap<>();
            if (!passive) {
                try {
                    initProperties = QueueUtil.generateTopicProperties(queueName, durable,
                            autoDelete, passive, arguments);
                } catch (Exception e) {
                    log.error("Failed to generate topic properties for exchange {} in vhost {}.",
                            queueName, namespaceName, e);
                    queueCompletableFuture.completeExceptionally(e);
                    removeQueueFuture(namespaceName, queueName);
                    return queueCompletableFuture;
                }
            }

            CompletableFuture<Topic> topicCompletableFuture =
                    amqpTopicManager.getTopic(topicName, !passive, initProperties);
            topicCompletableFuture.whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    log.error("[{}][{}] Failed to get queue topic.", namespaceName, queueName, throwable);
                    queueCompletableFuture.completeExceptionally(throwable);
                    removeQueueFuture(namespaceName, queueName);
                } else {
                    if (null == topic) {
                        log.warn("[{}][{}] Queue topic did not exist.", namespaceName, queueName);
                        queueCompletableFuture.complete(null);
                        removeQueueFuture(namespaceName, queueName);
                    } else {
                        // recover metadata if existed
                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                        Map<String, String> properties = persistentTopic.getManagedLedger().getProperties();

                        // TODO: reset connectionId, exclusive and autoDelete
                        PersistentQueue amqpQueue = new PersistentQueue(queueName, persistentTopic,
                                0, false, false, properties);
                        if (!config.isAmqpMultiBundleEnable()) {
                            try {
                                amqpQueue.recoverRoutersFromQueueProperties(properties, exchangeContainer,
                                        namespaceName);
                            } catch (Exception e) {
                                log.error("[{}][{}] Failed to recover routers for queue from properties.",
                                        namespaceName, queueName, e);
                                queueCompletableFuture.completeExceptionally(e);
                                removeQueueFuture(namespaceName, queueName);
                                return;
                            }
                        }
                        queueCompletableFuture.complete(amqpQueue);
                    }
                }
            });
        }
        return queueCompletableFuture;
    }

    /**
     * Delete the queue by namespace and exchange name.
     *
     * @param namespaceName namespace name in pulsar
     * @param queueName     name of queue
     */
    public void deleteQueue(NamespaceName namespaceName, String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            return;
        }
        removeQueueFuture(namespaceName, queueName);
    }

    private void removeQueueFuture(NamespaceName namespaceName, String queueName) {
        if (queueMap.containsKey(namespaceName)) {
            queueMap.get(namespaceName).remove(queueName);
        }
    }

}
