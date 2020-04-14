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

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.MessagePublishContext;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Persistent Exchange.
 */
public class PersistentExchange extends AbstractAmqpExchange {

    private PersistentTopic persistentTopic;

    public PersistentExchange(String exchangeName, Type type, PersistentTopic persistentTopic) {
        super(exchangeName, type, new HashSet<>(), true);
        this.persistentTopic = persistentTopic;
    }

    @Override
    public CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();
        List<CompletableFuture<Void>> routeFutures = new ArrayList<>(queues.size());
        Map<String, Object> properties = MessageConvertUtils.getHeaders(message);
        publishFuture.whenComplete((position, throwable) -> {
            if (throwable != null) {
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
        return null;
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String queueName, Position position) {
        return null;
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> markDeleteAsync(String queueName, Position position) {
        return null;
    }

    @Override
    public CompletableFuture<Position> getMarkDeleteAsync(String queueName) {
        return null;
    }
}
