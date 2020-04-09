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
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.MessagePublishContext;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Persistent queue.
 */
public class PersistentQueue extends AbstractAmqpQueue {

    private Map<String, PersistentTopic> indexTopics;

    public PersistentQueue(String queueName) {
        super(queueName, true);
        indexTopics = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        PersistentTopic persistentTopic = indexTopics.get(exchangeName);
        MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(PositionImpl.get(ledgerId, entryId));
        MessagePublishContext.publishMessages(message, persistentTopic, new CompletableFuture<>());
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId) {
        return null;
    }

    @Override
    public void bindExchange(AmqpExchange exchange, AmqpMessageRouter router, PersistentTopic persistentTopic) {
        super.bindExchange(exchange, router, persistentTopic);
        indexTopics.computeIfAbsent(exchange.getName(), exchangeName -> persistentTopic);
    }

    public static String getIndexTopicName(NamespaceName namespaceName, String exchangeName, String queueName) {
        return TopicName.get(TopicDomain.persistent.value(),
                namespaceName, exchangeName + "|" + queueName).toString();
    }

}
