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

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

/**
 * Interface of the AMQP queue.
 * The AMQP broker should maintaining queues in a Map, so that the broker can find the right queue to read messages.
 */
public interface AmqpQueue {

    /**
     * Get name of the queue.
     *
     * @return name of the queue.
     */
    String getName();

    boolean getDurable();

    /**
     * Write the index message into the queue.
     */
    CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId);

    /**
     * Read entry by queue message. Since the queue just store message IDs, so it's need to read the real data from
     * the exchange that the queue bind.
     *
     * @return entry of the message data.
     */
    CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId);

    /**
     * Acknowledge a message in the queue.
     */
    CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId);

    /**
     * Get message router by exchange name.
     *
     * @param exchangeName exchange name.
     * @return message router of the queue.
     */
    AmqpMessageRouter getRouter(String exchangeName);

    /**
     * Bind to a exchange {@link AmqpExchange}.
     */
    void bindExchange(AmqpExchange exchange, AmqpMessageRouter router, PersistentTopic persistentTopic);

    /**
     * UnBind a exchange for the queue.
     * @param exchange
     */
    void unbindExchange(AmqpExchange exchange);
}
