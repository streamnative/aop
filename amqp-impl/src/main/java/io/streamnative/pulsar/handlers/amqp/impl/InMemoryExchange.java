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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * In-memory implementation for {@link AmqpExchange}.
 */
public class InMemoryExchange extends AbstractAmqpExchange {

    private final TreeMap<PositionImpl, Entry> messageStore = new TreeMap<>();
    private final Map<String, TreeMap<PositionImpl, Object>> cursors = new ConcurrentHashMap<>();
    private final long currentLedgerId;
    private long currentEntryId;

    public InMemoryExchange(String exchangeName, AmqpExchange.Type exchangeType, boolean autoDelete) {
        super(exchangeName, exchangeType, new HashSet<>(), false, autoDelete, false, null);
        this.currentLedgerId = 1L;
    }

    public InMemoryExchange(String exchangeName, AmqpExchange.Type exchangeType, boolean autoDelete,
                            Map<String, Object> arguments) {
        super(exchangeName, exchangeType, new HashSet<>(), false, autoDelete, false, arguments);
        this.currentLedgerId = 1L;
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        Entry entry = EntryImpl.create(currentLedgerId, ++currentEntryId,
                MessageConvertUtils.messageToByteBuf(message));
        PositionImpl position = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
        messageStore.put(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()), entry);
        List<CompletableFuture<Void>> routeFutures = new ArrayList<>(queues.size());
        for (AmqpQueue queue : queues) {
            TreeMap<PositionImpl, Object> cursor = cursors.computeIfAbsent(queue.getName(), key -> new TreeMap<>());
            cursor.put(position, null);
            routeFutures.add(queue.getRouter(this.exchangeName).routingMessage(position.getLedgerId(),
                    position.getEntryId(), routingKey, null));
        }
        return FutureUtil.waitForAll(routeFutures).thenApply(v -> position);
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, long ledgerId, long entryId) {
        return readEntryAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, Position position) {
        TreeMap<PositionImpl, Object> cursor = cursors.get(queueName);
        if (cursor == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (!cursor.containsKey(position)) {
            return CompletableFuture.completedFuture(null);
        }
        Entry entry = messageStore.get(position);
        entry.getDataBuffer().resetReaderIndex();
        return CompletableFuture.completedFuture(entry);
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId) {
        return markDeleteAsync(queueName, PositionImpl.get(ledgerId, entryId));
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, Position position) {
        TreeMap<PositionImpl, Object> cursor = cursors.get(queueName);
        if (cursor == null) {
            return CompletableFuture.completedFuture(null);
        }
        cursor.subMap(PositionImpl.get(0, 0), true, (PositionImpl) position, true).clear();
        PositionImpl deletePosition = null;
        for (TreeMap<PositionImpl, Object> c : cursors.values()) {
            PositionImpl firstKey = c.firstKey();
            if (deletePosition == null) {
                deletePosition = PositionImpl.get(firstKey.getLedgerId(), firstKey.getEntryId() - 1);
            } else {
                if (firstKey.compareTo(deletePosition) < 0) {
                    deletePosition = PositionImpl.get(firstKey.getLedgerId(), firstKey.getEntryId() - 1);
                }
            }
        }
        if (deletePosition != null) {
            messageStore.subMap(PositionImpl.get(0, 0), true, deletePosition, true).clear();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Position> getMarkDeleteAsync(String queueName) {
        TreeMap<PositionImpl, Object> cursor = cursors.get(queueName);
        if (cursor == null) {
            return CompletableFuture.completedFuture(null);
        }
        PositionImpl first = cursor.firstKey();
        return CompletableFuture.completedFuture(PositionImpl.get(first.getLedgerId(), first.getEntryId() - 1));
    }

    @VisibleForTesting
    public int getMessages() {
        return messageStore.size();
    }

    @VisibleForTesting
    public CompletableFuture<Position> writeMessageAsync(ByteBuf byteBuf) {
        Entry entry = EntryImpl.create(currentLedgerId, ++currentEntryId, byteBuf);
        PositionImpl position = PositionImpl.get(entry.getLedgerId(), entry.getEntryId());
        messageStore.put(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()), entry);
        List<CompletableFuture<Void>> routeFutures = new ArrayList<>(queues.size());
        for (AmqpQueue queue : queues) {
            TreeMap<PositionImpl, Object> cursor = cursors.computeIfAbsent(queue.getName(), key -> new TreeMap<>());
            cursor.put(position, null);
            routeFutures.add(queue.getRouter(this.exchangeName).routingMessage(position.getLedgerId(),
                    position.getEntryId(), "", null));
        }
        return FutureUtil.waitForAll(routeFutures).thenApply(v -> position);
    }

    @Override
    public Topic getTopic() {
        return null;
    }

}
