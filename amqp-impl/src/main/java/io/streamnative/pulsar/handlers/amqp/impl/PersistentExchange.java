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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.AmqpTopicCursorManager;
import io.streamnative.pulsar.handlers.amqp.AmqpTopicManager;
import io.streamnative.pulsar.handlers.amqp.MessagePublishContext;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Persistent Exchange.
 */
@Slf4j
public class PersistentExchange extends AbstractAmqpExchange {
    public static final String EXCHANGE = "EXCHANGE";
    public static final String QUEUES = "QUEUES";
    public static final String TYPE = "TYPE";

    private PersistentTopic persistentTopic;
    private AmqpTopicCursorManager cursorManager;
    private ObjectMapper jsonMapper = ObjectMapperFactory.create();

    public PersistentExchange(String exchangeName, Type type, PersistentTopic persistentTopic, boolean autoDelete) {
        super(exchangeName, type, new HashSet<>(), true, autoDelete);
        this.persistentTopic = persistentTopic;
        updateExchangeProperties();
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> routeFutures = new ArrayList<>(queues.size());
        Map<String, Object> properties = MessageConvertUtils.getHeaders(message);
        publishFuture.whenComplete((position, throwable) -> {
            if (throwable != null) {
                log.error("Exchange persistent topic: {} failed.", persistentTopic.getName(), throwable);
                publishFuture.completeExceptionally(throwable);
            } else {
                for (AmqpQueue queue : queues) {
                    routeFutures.add(
                            queue.getRouter(getName()).routingMessage(
                                    ((PositionImpl) position).getLedgerId(),
                                    ((PositionImpl) position).getEntryId(),
                                    routingKey, properties)
                    );
                }
            }
        });
        FutureUtil.waitForAll(routeFutures);
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
        ManagedCursor cursor = getTopicCursorManager().getOrCreateCursor(queueName);
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
        ManagedCursor cursor = getTopicCursorManager().getCursor(queueName);
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
        ManagedCursor cursor = getTopicCursorManager().getCursor(queueName);
        if (cursor == null) {
            future.complete(null);
            return future;
        }
        future.complete(cursor.getMarkDeletedPosition());
        return future;
    }

    public AmqpTopicCursorManager getTopicCursorManager() {
        if (cursorManager == null) {
            try {
                cursorManager = AmqpTopicManager.getTopicCursorManager(persistentTopic.getName()).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return cursorManager;
    }

    @Override
    public void addQueue(AmqpQueue queue) {
        queues.add(queue);
        updateExchangeProperties();
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        queues.remove(queue);
        updateExchangeProperties();
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
            properties.put(QUEUES, jsonMapper.writeValueAsString(getQueueNames()));
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
}
