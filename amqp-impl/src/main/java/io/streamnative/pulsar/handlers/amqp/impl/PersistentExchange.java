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
package io.streamnative.pulsar.handlers.amqp.impl;

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.JSON_MAPPER;
import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.PROP_CUSTOM_JSON;
import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.PROP_EXCHANGE;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpBinding;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeProperties;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeReplicator;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.JsonUtil;
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
    public static final String DES_EXCHANGES = "DES_EXCHANGES";
    public static final String BINDINGS = "BINDINGS";
    public static final String TOPIC_PREFIX = "__amqp_exchange__";

    private PersistentTopic persistentTopic;
    private ObjectMapper jsonMapper = new JsonMapper();
    private final ConcurrentOpenHashMap<String, CompletableFuture<ManagedCursor>> cursors;
    private AmqpExchangeReplicator messageReplicator;
    private AmqpEntryWriter amqpEntryWriter;

    public PersistentExchange(String exchangeName, ExchangeType type, PersistentTopic persistentTopic,
                              boolean durable, boolean autoDelete, boolean internal, Map<String, Object> arguments,
                              Executor routeExecutor, int routeQueueSize) {
        super(exchangeName, type, Sets.newConcurrentHashSet(), Sets.newConcurrentHashSet(), durable, autoDelete,
                internal, arguments);
        this.persistentTopic = persistentTopic;
        topicNameValidate();
        cursors = new ConcurrentOpenHashMap<>(16, 1);
        for (ManagedCursor cursor : persistentTopic.getManagedLedger().getCursors()) {
            cursors.put(cursor.getName(), CompletableFuture.completedFuture(cursor));
            log.info("PersistentExchange {} recover cursor {}", persistentTopic.getName(), cursor.toString());
            cursor.setInactive();
        }

        if (messageReplicator == null) {
            messageReplicator = new AmqpExchangeReplicator(this, routeExecutor, routeQueueSize) {
                @Override
                public CompletableFuture<Void> readProcess(ByteBuf data, Position position) {
                    Map<String, Object> props = new HashMap<>();
                    try {
                        MessageImpl<byte[]> message = MessageImpl.deserialize(data);
                        for (KeyValue keyValue : message.getMessageBuilder().getPropertiesList()) {
                            props.put(keyValue.getKey(), keyValue.getValue());
                            if (keyValue.getKey().equals(PROP_CUSTOM_JSON)) {
                                props.putAll(JsonUtil.fromJson(keyValue.getValue(), Map.class));
                            }
                        }
                        Collection<CompletableFuture<Void>> routeFutureList = new ArrayList<>();
                        String bindingKey = props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString();
                        if (exchangeType == ExchangeType.DIRECT) {
                            // route to queue
                            Set<AmqpQueue> queueSet = bindingKeyQueueMap.getOrDefault(
                                    bindingKey, Collections.EMPTY_SET);
                            for (AmqpQueue queue : queueSet) {
                                routeFutureList.add(
                                        queue.writeIndexMessageAsync(exchangeName, position.getLedgerId(),
                                                position.getEntryId(), props));
                            }
                            // route to exchange
                            Set<AmqpExchange> exchangeSet = bindingKeyExchangeMap.getOrDefault(
                                    bindingKey, Collections.EMPTY_SET);
                            for (AmqpExchange exchange : exchangeSet) {
                                if (!props.getOrDefault(PROP_EXCHANGE, "").equals(exchange.getName())) {
                                    // indicate this message is from the destination exchange
                                    // don't need to route, avoid dead loop
                                    continue;
                                }
                                routeFutureList.add(exchange.writeMessageAsync(message, null)
                                        .thenApply(__ -> null));
                            }
                        } else if (exchangeType == ExchangeType.FANOUT) {
                            for (AmqpQueue queue : queues) {
                                routeFutureList.add(
                                        queue.writeIndexMessageAsync(
                                                exchangeName, position.getLedgerId(), position.getEntryId(), props));
                            }
                            for (AmqpExchange exchange : exchanges) {
                                if (!props.getOrDefault(PROP_EXCHANGE, "").equals(exchange.getName())) {
                                    // indicate this message is from the destination exchange
                                    // don't need to route, avoid dead loop
                                    continue;
                                }
                                routeFutureList.add(exchange.writeMessageAsync(message, null)
                                        .thenApply(__ -> null));
                            }
                        } else {
                            for (AmqpQueue queue : queues) {
                                CompletableFuture<Void> routeFuture = queue.getRouter(exchangeName).routingMessage(
                                        position.getLedgerId(), position.getEntryId(),
                                        bindingKey,
                                        props);
                                routeFutureList.add(routeFuture);
                            }
                            for (AmqpExchange exchange : exchanges) {
                                CompletableFuture<Void> routeFuture = exchange.getRouter(exchangeName)
                                        .routingMessageToEx(data, bindingKey,
                                                message.getMessageBuilder().getPropertiesList(), props);
                                routeFutureList.add(routeFuture);
                            }
                        }

                        for (AmqpExchange exchange : exchanges) {
                            if (props.getOrDefault(PROP_EXCHANGE, "").equals(exchange.getName())) {
                                // indicate this message is from the destination exchange
                                // don't need to route, avoid dead loop
                                continue;
                            }
                            props.put(PROP_EXCHANGE, exchange.getName());
                            CompletableFuture<Void> routeFuture = exchange.getRouter(exchangeName).routingMessageToEx(
                                    data,
                                    bindingKey,
                                    message.getMessageBuilder().getPropertiesList(),
                                    props
                            );
                            routeFutureList.add(routeFuture);
                        }

                        return FutureUtil.waitForAll(routeFutureList);
                    } catch (Exception e) {
                        log.error("Read process failed. exchangeName: {}", exchangeName, e);
                        return FutureUtil.failedFuture(e);
                    }
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
        if (exchangeType == ExchangeType.DIRECT) {
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
        if (exchangeType == ExchangeType.DIRECT) {
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
    public CompletableFuture<Void> bindExchange(AmqpExchange sourceEx, String routingKey, Map<String, Object> params) {
        routerMap.compute(sourceEx.getName(), (k, router) -> {
            if (router == null) {
                router = AbstractAmqpMessageRouter.generateRouter(sourceEx.getType());
            }
            AmqpBinding binding = new AmqpBinding(sourceEx.getName(), routingKey, params);
            router.addBinding(binding);
            router.setExchange(sourceEx);
            router.setDestinationExchange(this);
            router.setArguments(params);
            return router;
        });
        updateExchangeProperties();
        return sourceEx.addExchange(this, routingKey, params);
    }

    @Override
    public CompletableFuture<Void> unbindExchange(AmqpExchange sourceEx, String routingKey,
                                                  Map<String, Object> params) {
        routerMap.computeIfPresent(sourceEx.getName(), (k, router) -> {
            router.getBindings().remove(new AmqpBinding(sourceEx.getName(), routingKey, params).propsKey());
            return router;
        });
        updateExchangeProperties();
        sourceEx.removeExchange(this, routingKey, params);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> addExchange(AmqpExchange destinationEx, String routingKey,
                                               Map<String, Object> params) {
        exchanges.add(destinationEx);
        if (exchangeType == ExchangeType.DIRECT) {
            bindingKeyExchangeMap.compute(routingKey, (k, v) -> {
                if (v == null) {
                    v = Sets.newConcurrentHashSet();
                }
                v.add(destinationEx);
                return v;
            });
        }
        createCursorIfNotExists(getExBindCursorName(destinationEx));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void removeExchange(AmqpExchange destinationEx, String routingKey, Map<String, Object> params) {
        if (exchangeType == ExchangeType.DIRECT) {
            bindingKeyExchangeMap.computeIfPresent(routingKey, (k, v) -> {
                v.remove(destinationEx);
                if (v.isEmpty()) {
                    return null;
                }
                return v;
            });
        }
        if (routerMap.get(destinationEx.getName()) == null
                || routerMap.get(destinationEx.getName()).getBindings().isEmpty()) {
            exchanges.remove(destinationEx);
            deleteCursor(getExBindCursorName(destinationEx));
        }
    }

    private String getExBindCursorName(AmqpExchange exchange) {
        return "__des_ex_" + exchange.getName();
    }

    @Override
    public int getExchangeSize() {
        return exchanges.size();
    }

    @Override
    public AmqpMessageRouter getRouter(String sourceEx) {
        return routerMap.get(sourceEx);
    }

    @Override
    public Topic getTopic(){
        return persistentTopic;
    }

    private void updateExchangeProperties() {
        Map<String, String> properties = this.persistentTopic.getManagedLedger().getProperties();
        try {
            List<String> queueNames = getQueueNames();
            if (queueNames.size() != 0) {
                properties.put(QUEUES, JSON_MAPPER.writeValueAsString(getQueueNames()));
            }
            properties.put(DES_EXCHANGES, jsonMapper.writeValueAsString(getExchangeNames()));
            properties.put(BINDINGS, jsonMapper.writeValueAsString(getBindingsData()));
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

    private List<String> getExchangeNames() {
        return exchanges.stream().map(AmqpExchange::getName).collect(Collectors.toList());
    }

    private List<AmqpExchangeProperties> getBindingsData() {
        List<AmqpExchangeProperties> propertiesList = new ArrayList<>(routerMap.size());
        for (AmqpMessageRouter router : routerMap.values()) {
            AmqpExchangeProperties properties = new AmqpExchangeProperties();
            properties.setExchangeName(router.getExchange().getName());
            properties.setType(router.getExchange().getType());
            properties.setBindings(router.getBindings());
            properties.setArguments(router.getArguments());
            propertiesList.add(properties);
        }
        return propertiesList;
    }

    public void recover(Map<String, String> properties, ExchangeContainer exchangeContainer,
                        NamespaceName namespaceName) throws JsonProcessingException {
        if (null == properties || properties.isEmpty() || !properties.containsKey(BINDINGS)) {
            return;
        }
        List<AmqpExchangeProperties> amqpExchangeProperties = jsonMapper.readValue(properties.get(BINDINGS),
                new TypeReference<List<AmqpExchangeProperties>>() {});
        if (amqpExchangeProperties == null) {
            return;
        }
        for (AmqpExchangeProperties props : amqpExchangeProperties) {
            exchangeContainer.asyncGetExchange(namespaceName, props.getExchangeName(), false, null)
                    .thenAccept(ex -> {
                        AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(props.getType());
                        messageRouter.setExchange(ex);
                        messageRouter.setDestinationExchange(this);
                        messageRouter.setBindings(props.getBindings());
                        messageRouter.setArguments(props.getArguments());
                    });
        }
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

}
