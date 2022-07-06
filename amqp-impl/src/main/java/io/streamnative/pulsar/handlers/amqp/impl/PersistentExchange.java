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
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerService;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeReplicator;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MapperUtil;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
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
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
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

    private final PersistentTopic persistentTopic;
    private final ConcurrentOpenHashMap<String, CompletableFuture<ManagedCursor>> cursors;
    private final AmqpExchangeReplicator messageReplicator;
    private final AmqpEntryWriter amqpEntryWriter;
    private final AmqpBrokerService amqpBrokerService;

    private final Backoff startReplicateBackoff = new Backoff(100, TimeUnit.MILLISECONDS,
            30, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);

    public PersistentExchange(AmqpBrokerService amqpBrokerService, String exchangeName, Type type,
                              PersistentTopic persistentTopic, boolean durable, boolean autoDelete, boolean internal,
                              Map<String, Object> arguments, Executor routeExecutor, int routeQueueSize) {
        super(exchangeName, type, new HashSet<>(), durable, autoDelete, internal, arguments);
        this.amqpBrokerService = amqpBrokerService;
        this.persistentTopic = persistentTopic;
        topicNameValidate();
        cursors = ConcurrentOpenHashMap.<String, CompletableFuture<ManagedCursor>>newBuilder()
                .expectedItems(16).concurrencyLevel(1).build();
        for (ManagedCursor cursor : persistentTopic.getManagedLedger().getCursors()) {
            cursors.put(cursor.getName(), CompletableFuture.completedFuture(cursor));
            log.info("[{}] PersistentExchange recover cursor {}", persistentTopic.getName(), cursor.toString());
            cursor.setInactive();
        }

        this.amqpEntryWriter = new AmqpEntryWriter(persistentTopic);
        this.messageReplicator = new AmqpExchangeReplicator(this, routeExecutor, routeQueueSize);
        // start routing message could be in async way
        startRouteMessages();
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
        updateExchangeProperties();
        return createCursorIfNotExists(queue.getName()).thenApply(__ -> null);
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        queues.remove(queue);
        updateExchangeProperties();
        deleteCursor(queue.getName());
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
        return cursors.computeIfAbsent(name, cursor -> {
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

    private void startRouteMessages() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        startRouteMessagesInternal(future);
    }

    private void startRouteMessagesInternal(CompletableFuture<Void> future) {
        this.recoverAllBoundQueues()
                .thenAccept(__ -> {
                    this.messageReplicator.startReplicate();
                    future.complete(null);
                })
                .exceptionally(t -> {
                    this.messageReplicator.stopReplicate();
                    long backoff = this.startReplicateBackoff.next();
                    log.error("[{}] Failed to start route messages, retry in {} ms.", exchangeName, backoff, t);
                    amqpBrokerService.getPulsarService().getExecutor()
                            .schedule(() -> this.startRouteMessagesInternal(future), backoff, TimeUnit.MILLISECONDS);
                    return null;
                });
    }

    public CompletableFuture<Void> recoverAllBoundQueues() {
        TopicName topicName = TopicName.get(persistentTopic.getName());
        String queueJsonStr = persistentTopic.getManagedLedger().getProperties().get(PersistentExchange.QUEUES);
        List<CompletableFuture<AmqpQueue>> amqpQueueFutureList = new ArrayList<>();
        if (queueJsonStr != null) {
            try {
                List<String> list = MapperUtil.readListValue(queueJsonStr);
                for (String queue : list) {
                    amqpQueueFutureList.add(
                            this.amqpBrokerService.getQueueContainer()
                                    .asyncGetQueue(topicName.getNamespaceObject(), queue, true));
                }
            } catch (Exception e) {
                log.error("Failed to read bound queues for exchange {}.", exchangeName, e);
                FutureUtil.failedFuture(e);
            }
        }
        return FutureUtil.waitForAll(amqpQueueFutureList);
    }

}
