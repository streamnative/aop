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

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpExchangeReplicator;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.MessagePublishContext;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
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
    public static final String TOPIC_PREFIX = "__amqp_exchange__";

    private PersistentTopic persistentTopic;
    private ObjectMapper jsonMapper = new JsonMapper();
    private final ConcurrentOpenHashMap<String, ManagedCursor> cursors;
    private AmqpExchangeReplicator messageReplicator;

    public PersistentExchange(String exchangeName, Type type, PersistentTopic persistentTopic, boolean autoDelete) {
        super(exchangeName, type, new HashSet<>(), true, autoDelete);
        this.persistentTopic = persistentTopic;
        topicNameValidate();
        updateExchangeProperties();
        cursors = new ConcurrentOpenHashMap<>(16, 1);
        for (ManagedCursor cursor : persistentTopic.getManagedLedger().getCursors()) {
            cursors.put(cursor.getName(), cursor);
            log.info("PersistentExchange {} recover cursor {}", persistentTopic.getName(), cursor.toString());
            cursor.setInactive();
        }

        if (messageReplicator == null) {
            messageReplicator = new AmqpExchangeReplicator(this) {
                @Override
                public CompletableFuture<Void> readProcess(Entry entry) {
                    Map<String, Object> props;
                    try {
                        MessageImpl<byte[]> message = MessageImpl.deserialize(entry.getDataBuffer());
                        props = message.getMessageBuilder().getPropertiesList().stream()
                                .collect(Collectors.toMap(PulsarApi.KeyValue::getKey, PulsarApi.KeyValue::getValue));
                    } catch (IOException e) {
                        log.error("Deserialize entry dataBuffer failed. exchangeName: {}", exchangeName, e);
                        return FutureUtil.failedFuture(e);
                    }
                    List<CompletableFuture<Void>> routeFutureList = new ArrayList<>();
                    for (AmqpQueue queue : queues) {
                        CompletableFuture<Void> routeFuture = queue.getRouter(exchangeName).routingMessage(
                                entry.getLedgerId(), entry.getEntryId(),
                                props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
                                props);
                        routeFutureList.add(routeFuture);
                    }
                    return FutureUtil.waitForAll(routeFutureList);
                }
            };
            messageReplicator.startReplicate();
        }
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();
        MessagePublishContext.publishMessages(message, persistentTopic, publishFuture);
        return publishFuture;
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, long ledgerId, long entryId) {
        return readEntryAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, Position position) {
        CompletableFuture<Entry> future = new CompletableFuture();
        // TODO Temporarily put the creation operation here, and later put the operation in router
        ManagedCursor cursor = cursors.get(queueName);
        if (cursor == null) {
            future.completeExceptionally(new ManagedLedgerException("cursor is null"));
            return future;
        }
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) cursor.getManagedLedger();

        ledger.asyncReadEntry((PositionImpl) position, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(Entry entry, Object o) {
                    future.complete(entry);
                }

                @Override
                public void readEntryFailed(ManagedLedgerException e, Object o) {
                    future.completeExceptionally(e);
                }
            }
            , null);
        return future;
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId) {
        return markDeleteAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, Position position) {
        CompletableFuture<Void> future = new CompletableFuture();
        ManagedCursor cursor = cursors.get(queueName);
        if (cursor == null) {
            future.complete(null);
            return future;
        }
        if (((PositionImpl) position).compareTo((PositionImpl) cursor.getMarkDeletedPosition()) < 0) {
            future.complete(null);
            return future;
        }
        cursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
            @Override
            public void markDeleteComplete(Object ctx) {
                if (log.isDebugEnabled()) {
                    log.debug("Mark delete success for position: {}", position);
                }
                future.complete(null);
            }

            @Override
            public void markDeleteFailed(ManagedLedgerException e, Object ctx) {
                log.warn("Mark delete success for position: {} with error:",
                    position, e);
                future.completeExceptionally(e);
            }
        }, null);
        return future;
    }

    @Override
    public CompletableFuture<Position> getMarkDeleteAsync(String queueName) {
        CompletableFuture<Position> future = new CompletableFuture();
        ManagedCursor cursor = cursors.get(queueName);
        if (cursor == null) {
            future.complete(null);
            return future;
        }
        future.complete(cursor.getMarkDeletedPosition());
        return future;
    }


    @Override
    public void addQueue(AmqpQueue queue) {
        queues.add(queue);
        updateExchangeProperties();
        createCursorIfNotExists(queue.getName());

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
        Map<String, String> properties = new HashMap<>();
        try {
            properties.put(EXCHANGE, exchangeName);
            properties.put(TYPE, exchangeType.toString());
            List<String> queueNames = getQueueNames();
            if (queueNames.size() != 0) {
                properties.put(QUEUES, jsonMapper.writeValueAsString(getQueueNames()));
            }
        } catch (JsonProcessingException e) {
            log.error("[{}] covert map of routers to String error: {}", exchangeName, e.getMessage());
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

    private ManagedCursor createCursorIfNotExists(String name) {
        return cursors.computeIfAbsent(name, cusrsor -> {
            ManagedLedgerImpl ledger = (ManagedLedgerImpl) persistentTopic.getManagedLedger();
            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for topic {}", name, persistentTopic.getName());
            }
            ManagedCursor newCursor;
            try {
                //newCursor = ledger.openCursor(name, PulsarApi.CommandSubscribe.InitialPosition.Latest);
                newCursor = ledger.newNonDurableCursor(ledger.getLastConfirmedEntry(), name);
            } catch (ManagedLedgerException e) {
                log.error("Error new cursor for topic {} - {}. will cause fetch data error.",
                    persistentTopic.getName(), e);
                return null;
            }
            return newCursor;
        });
    }

    public void deleteCursor(String name) {
        ManagedCursor cursor = cursors.remove(name);
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
                    log.error("[{}] Error deleting cursor {} for topic {} for reason: {}.",
                        cursor.getName(), persistentTopic.getName(), exception);
                }
            }, null);
        }

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
