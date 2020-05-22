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

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpQueue;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;

/**
 * In memory implementation for AMQP queue.
 */
public class InMemoryQueue extends AbstractAmqpQueue {

    private final Map<String, LinkedList<PositionImpl>> indexStore = new ConcurrentHashMap<>();

    public InMemoryQueue(String queueName, long connectionId) {
        super(queueName, false, connectionId);
    }

    public InMemoryQueue(String queueName, long connectionId, boolean exclusive, boolean autoDelete) {
        super(queueName, false, connectionId, exclusive, autoDelete);
    }

    @Override
    public CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId) {
        List<PositionImpl> positions = indexStore.computeIfAbsent(exchangeName, (key) -> new LinkedList<>());
        positions.add(PositionImpl.get(ledgerId, entryId));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId) {
        LinkedList<PositionImpl> indexes = indexStore.get(exchangeName);
        if (indexes == null || indexes.size() == 0 || !indexes.contains(PositionImpl.get(ledgerId, entryId))) {
            return CompletableFuture.completedFuture(null);
        }
        return routers.get(exchangeName).getExchange().readEntryAsync(this.queueName, ledgerId, entryId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId) {
        LinkedList<PositionImpl> positions = indexStore.get(exchangeName);
        if (positions != null && positions.size() > 0) {
            positions.remove(PositionImpl.get(ledgerId, entryId));
            PositionImpl markDeletePosition = positions.getFirst();
            return routers.get(exchangeName).getExchange().markDeleteAsync(this.queueName,
                    markDeletePosition.getLedgerId(), markDeletePosition.getEntryId() - 1);
        }
        return CompletableFuture.completedFuture(null);
    }
}
