/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.impl;

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.JSON_MAPPER;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeReplicator;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.ExchangeMessageRouter;
import io.streamnative.pulsar.handlers.amqp.admin.AmqpAdmin;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

/**
 * Persistent Exchange.
 */
@Slf4j
public class PersistentExchange extends AbstractAmqpExchange {
    public static final String EXCHANGE = "EXCHANGE";
    public static final String QUEUES = "QUEUES";
    public static final String TYPE = "TYPE";
    public static final String DURABLE = "DURABLE";
    public static final String AUTO_DELETE = "AUTO_DELETE";
    public static final String INTERNAL = "INTERNAL";
    public static final String ARGUMENTS = "ARGUMENTS";
    public static final String TOPIC_PREFIX = "__amqp_exchange__";
    private static final String BINDINGS = "BINDINGS";
    public static final String X_DELAYED_TYPE = "x-delayed-type";
    @Getter
    private final boolean existDelayedType;

    private PersistentTopic persistentTopic;
    private ConcurrentOpenHashMap<String, CompletableFuture<ManagedCursor>> cursors;
    private AmqpExchangeReplicator messageReplicator;
    private AmqpEntryWriter amqpEntryWriter;

    private ExchangeMessageRouter exchangeMessageRouter;
    @Getter
    private Set<Binding> bindings;

    @Getter
    private PulsarClient pulsarClient;

    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode(exclude = {"source", "arguments"})
    @Data
    public static class Binding implements Serializable {
        private String des;
        private String desType;
        private String key;
        private String source;
        private Map<String, Object> arguments;
    }

    public PersistentExchange(String exchangeName, Map<String, String> properties, Type type,
                              PersistentTopic persistentTopic,
                              boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments,
                              ExecutorService routeExecutor, int routeQueueSize, boolean amqpMultiBundleEnable,
                              AmqpAdmin amqpAdmin, PulsarClient pulsarClient)
            throws JsonProcessingException {
        super(exchangeName, type, Sets.newConcurrentHashSet(), durable, autoDelete, internal, arguments, properties);
        this.pulsarClient = pulsarClient;
        this.persistentTopic = persistentTopic;
        this.existDelayedType = arguments != null && arguments.containsKey(X_DELAYED_TYPE);
        topicNameValidate();

        if (amqpMultiBundleEnable) {
            bindings = Sets.newConcurrentHashSet();
            if (persistentTopic.getManagedLedger().getProperties().containsKey(BINDINGS)) {
                List<Binding> amqpQueueProperties = JSON_MAPPER.readValue(
                        persistentTopic.getManagedLedger().getProperties().get(BINDINGS), new TypeReference<>() {
                        });
                this.bindings.addAll(amqpQueueProperties);
            }
            this.exchangeMessageRouter = ExchangeMessageRouter.getInstance(this, routeExecutor);
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            NamespaceName namespaceName = TopicName.get(persistentTopic.getName()).getNamespaceObject();
            for (Binding binding : this.bindings) {
                // The initialization queue triggers the message expiration detection task
                if ("queue".equals(binding.desType)) {
                    futures.add(amqpAdmin.loadQueue(namespaceName, binding.des));
                }
                this.exchangeMessageRouter.addBinding(binding.des, binding.desType, binding.key, binding.arguments);
            }
            FutureUtil.waitForAll(futures).whenComplete((__, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to init queue [{}]", exchangeName, throwable);
                }
                this.exchangeMessageRouter.start();
            });
            return;
        }

        if (messageReplicator == null) {
            messageReplicator = new AmqpExchangeReplicator(this, routeExecutor, routeQueueSize) {
                @Override
                public CompletableFuture<Void> readProcess(ByteBuf data, Position position) {
                    Map<String, Object> props;
                    try {
                        MessageImpl<byte[]> message = MessageImpl.deserialize(data);
                        props = message.getMessageBuilder().getPropertiesList().stream()
                                .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                    } catch (Exception e) {
                        log.error("Failed to deserialize entry dataBuffer. exchangeName: {}", exchangeName, e);
                        return FutureUtil.failedFuture(e);
                    }

                    List<CompletableFuture<Void>> routeFutureList = new ArrayList<>();
                    if (exchangeType == Type.Direct) {
                        String bindingKey = props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString();
                        Set<AmqpQueue> queueSet = bindingKeyQueueMap.get(bindingKey);
                        if (queueSet == null) {
                            if (log.isDebugEnabled()) {
                                log.debug("The queue set of the bindingKey {} is not exist.", bindingKey);
                            }
                        } else {
                            for (AmqpQueue queue : queueSet) {
                                routeFutureList.add(
                                        queue.writeIndexMessageAsync(
                                                exchangeName, position.getLedgerId(), position.getEntryId(), props));
                            }
                        }
                    } else if (exchangeType == Type.Fanout) {
                        for (AmqpQueue queue : queues) {
                            routeFutureList.add(
                                    queue.writeIndexMessageAsync(
                                            exchangeName, position.getLedgerId(), position.getEntryId(), props));
                        }
                    } else {
                        for (AmqpQueue queue : queues) {
                            CompletableFuture<Void> routeFuture = queue.getRouter(exchangeName).routingMessage(
                                    position.getLedgerId(), position.getEntryId(),
                                    props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
                                    props);
                            routeFutureList.add(routeFuture);
                        }
                    }
                    return FutureUtil.waitForAll(routeFutureList);
                }
            };
            messageReplicator.startReplicate();
        }
        this.amqpEntryWriter = new AmqpEntryWriter(persistentTopic);
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        return amqpEntryWriter.publishMessage(message);
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, long ledgerId, long entryId) {
        return readEntryAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, Position position) {
        // TODO Temporarily put the creation operation here, and later put the operation in router
        CompletableFuture<Entry> future = new CompletableFuture<>();
        ((ManagedLedgerImpl) persistentTopic.getManagedLedger())
                .asyncReadEntry((PositionImpl) position, new AsyncCallbacks.ReadEntryCallback() {
                    @Override
                    public void readEntryComplete(Entry entry, Object o) {
                        future.complete(entry);
                    }

                    @Override
                    public void readEntryFailed(ManagedLedgerException e, Object o) {
                        future.completeExceptionally(e);
                    }
                }, null);
        return future;
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId) {
        return markDeleteAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        getCursor(queueName).thenAccept(cursor -> {
            if (cursor == null) {
                future.completeExceptionally(new RuntimeException("Failed to make delete, the cursor "
                        + queueName + " of the exchange " + exchangeName + " is null."));
                return;
            }
            if (((PositionImpl) position).compareTo((PositionImpl) cursor.getMarkDeletedPosition()) < 0) {
                future.complete(null);
                return;
            }
            cursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
                @Override
                public void markDeleteComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Mark delete success for position: {}", exchangeName, position);
                    }
                    future.complete(null);
                }

                @Override
                public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                    if (((PositionImpl) position).compareTo((PositionImpl) cursor.getMarkDeletedPosition()) < 0) {
                        log.warn("Mark delete failed for position: {}, {}", position, e.getMessage());
                    } else {
                        log.error("Mark delete failed for position: {}", position, e);
                    }
                    future.completeExceptionally(e);
                }
            }, null);
        }).exceptionally(t -> {
            future.completeExceptionally(t);
            return null;
        });
        return future;
    }

    @Override
    public CompletableFuture<Position> getMarkDeleteAsync(String queueName) {
        return getCursor(queueName).thenApply(ManagedCursor::getMarkDeletedPosition);
    }

    private CompletableFuture<ManagedCursor> getCursor(String queueName) {
        CompletableFuture<ManagedCursor> cursorFuture = cursors.get(queueName);
        if (cursorFuture == null) {
            return FutureUtil.failedFuture(new RuntimeException(
                    "The cursor " + queueName + " of the exchange " + exchangeName + " is not exist."));
        }
        return cursorFuture;
    }

    @Override
    public CompletableFuture<Void> addQueue(AmqpQueue queue) {
        queues.add(queue);
        if (exchangeType == Type.Direct) {
            for (String bindingKey : queue.getRouter(exchangeName).getBindingKey()) {
                bindingKeyQueueMap.compute(bindingKey, (k, v) -> {
                    if (v == null) {
                        Set<AmqpQueue> set = new HashSet<>();
                        set.add(queue);
                        return set;
                    } else {
                        v.add(queue);
                        return v;
                    }
                });
            }
        }
        updateExchangeProperties();
        return createCursorIfNotExists(queue.getName()).thenApply(__ -> null);
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        queues.remove(queue);
        if (exchangeType == Type.Direct) {
            for (Map.Entry<String, Set<AmqpQueue>> entry : bindingKeyQueueMap.entrySet()) {
                bindingKeyQueueMap.computeIfPresent(entry.getKey(), (k, v) -> {
                    v.remove(queue);
                    if (v.isEmpty()) {
                        return null;
                    }
                    return v;
                });
            }
        }
        updateExchangeProperties();
        deleteCursor(queue.getName());
    }

    @Override
    public Topic getTopic() {
        return persistentTopic;
    }

    private void updateExchangeProperties() {
        Map<String, String> properties = this.persistentTopic.getManagedLedger().getProperties();
        try {
            List<String> queueNames = getQueueNames();
            if (queueNames.size() != 0) {
                properties.put(QUEUES, JSON_MAPPER.writeValueAsString(getQueueNames()));
            }
        } catch (JsonProcessingException e) {
            log.error("[{}] covert queue list to String error: {}", exchangeName, e.getMessage());
            return;
        }
        PulsarTopicMetadataUtils.updateMetaData(this.persistentTopic, properties, exchangeName);
    }

    private List<String> getQueueNames() {
        List<String> queueNames = new ArrayList<>();
        for (AmqpQueue queue : queues) {
            queueNames.add(queue.getName());
        }
        return queueNames;
    }

    private CompletableFuture<ManagedCursor> createCursorIfNotExists(String name) {
        CompletableFuture<ManagedCursor> cursorFuture = new CompletableFuture<>();
        return cursors.computeIfAbsent(name, cusrsor -> {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for topic {}", name, persistentTopic.getName());
            }
            ledger.asyncOpenCursor(name, CommandSubscribe.InitialPosition.Earliest,
                    new AsyncCallbacks.OpenCursorCallback() {
                        @Override
                        public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                            cursorFuture.complete(cursor);
                        }

                        @Override
                        public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("[{}] Failed to open cursor. ", name, exception);
                            cursorFuture.completeExceptionally(exception);
                            if (cursors.get(name) != null && cursors.get(name).isCompletedExceptionally()
                                    || cursors.get(name).isCancelled()) {
                                cursors.remove(name);
                            }
                        }
                    }, null);
            return cursorFuture;
        });
    }

    public void deleteCursor(String name) {
        CompletableFuture<ManagedCursor> cursorFuture = cursors.remove(name);
        if (cursorFuture == null) {
            log.warn("Failed to delete cursor, the cursor {} of the exchange {} is not exist.", name, exchangeName);
            return;
        }
        cursorFuture.thenAccept(cursor -> {
            if (cursor != null) {
                persistentTopic.getManagedLedger().asyncDeleteCursor(cursor.getName(),
                        new AsyncCallbacks.DeleteCursorCallback() {
                            @Override
                            public void deleteCursorComplete(Object ctx) {
                                if (log.isDebugEnabled()) {
                                    log.debug("Cursor {} for topic {} deleted successfully .",
                                            cursor.getName(), persistentTopic.getName());
                                }
                            }

                            @Override
                            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                                log.error("[{}] Error deleting cursor for topic {}.",
                                        cursor.getName(), persistentTopic.getName(), exception);
                            }
                        }, null);
            }
        });
    }

    public static String getExchangeTopicName(NamespaceName namespaceName, String exchangeName) {
        return TopicName.get(TopicDomain.persistent.value(),
                namespaceName, TOPIC_PREFIX + exchangeName).toString();
    }

    public void topicNameValidate() {
        String[] nameArr = this.persistentTopic.getName().split("/");
        checkArgument(nameArr[nameArr.length - 1].equals(TOPIC_PREFIX + exchangeName),
                "The exchange topic name does not conform to the rules(__amqp_exchange__exchangeName).");
    }

    @Override
    public CompletableFuture<Void> queueBind(String queue, String routingKey, Map<String, Object> arguments) {
        this.bindings.add(new Binding(queue, "queue", routingKey, exchangeName, arguments));
        String bindingsJson;
        try {
            bindingsJson = JSON_MAPPER.writeValueAsString(this.bindings);
        } catch (JsonProcessingException e) {
            log.error("Failed to bind queue {} to exchange {}", queue, exchangeName, e);
            return FutureUtil.failedFuture(e);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.persistentTopic.getManagedLedger().asyncSetProperty(BINDINGS, bindingsJson,
                new AsyncCallbacks.UpdatePropertiesCallback() {
                    @Override
                    public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                        PersistentExchange.this.exchangeMessageRouter.addBinding(queue, "queue", routingKey, arguments);
                        future.complete(null);
                    }

                    @Override
                    public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to save binding metadata for bind operation.", exception);
                        future.completeExceptionally(exception);
                    }
                }, null);
        return future;
    }

    @Override
    public CompletableFuture<Void> queueUnBind(String queue, String routingKey, Map<String, Object> arguments) {
        this.bindings.remove(new Binding(queue, "queue", routingKey, exchangeName, arguments));
        String bindingsJson;
        try {
            bindingsJson = JSON_MAPPER.writeValueAsString(this.bindings);
        } catch (JsonProcessingException e) {
            log.error("Failed to unbind queue {} to exchange {}", queue, exchangeName, e);
            return FutureUtil.failedFuture(e);
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.persistentTopic.getManagedLedger().asyncSetProperty("BINDINGS", bindingsJson,
                new AsyncCallbacks.UpdatePropertiesCallback() {
                    @Override
                    public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                        PersistentExchange.this.exchangeMessageRouter.removeBinding(queue, "queue",
                                routingKey, arguments);
                        future.complete(null);
                    }

                    @Override
                    public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to save binding metadata for unbind operation.", exception);
                        future.completeExceptionally(exception);
                    }
                }, null);
        return future;
    }

    @Override
    public void close() {
        if (exchangeMessageRouter != null) {
            exchangeMessageRouter.close();
        }
    }
}
