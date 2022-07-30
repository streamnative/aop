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

import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.Message;

/**
 * Interface of the AMQP exchange.
 * The AMQP broker should maintaining exchanges in a Map, so that the broker can find the right exchange
 * to write messages and read messages.
 */
public interface AmqpExchange {

    /**
     * Get the name of the exchange.
     * The exchange name is the identify of an exchange.
     * @return name of the exchange.
     */
    String getName();

    boolean getDurable();

    boolean getAutoDelete();

    boolean getInternal();

    Map<String, Object> getArguments();

    Topic getTopic();

    /**
     * Get the type {@link ExchangeType} of the exchange.
     * @return the type of the exchange.
     */
    ExchangeType getType();

    /**
     * Write AMQP message to the exchange.
     *
     * @param message Pulsar message
     * @param routingKey
     */
    CompletableFuture<Position> writeMessageAsync(Message<byte[]> message, String routingKey);

    /**
     * Read entry {@link Entry} from the exchange.
     *
     * @param queueName name of the queue that read entry from the exchange.
     * @param ledgerId ledger ID of the entry that to read.
     * @param entryId entry ID of the entry that to read.
     * @return entry
     */
    CompletableFuture<Entry> readEntryAsync(String queueName, long ledgerId, long entryId);

    /**
     * Read entry {@link Entry} from the exchange.
     *
     * @param queueName name of the queue that read entry from the exchange.
     * @param position position of the entry that to read.
     * @return entry
     */
    CompletableFuture<Entry> readEntryAsync(String queueName, Position position);

    /**
     * Mark delete position for a queue.
     *
     * @param queueName name of the queue that to mark delete.
     * @param ledgerId ledger ID that to mark for the queue queue.
     * @param entryId entryId ID that to mark for the queue queue.
     */
    CompletableFuture<Void> markDeleteAsync(String queueName, long ledgerId, long entryId);

    /**
     * Mark delete position for a queue.
     *
     * @param queueName name of the queue that to mark delete.
     * @param position position that to mark for the queue queue.
     */
    CompletableFuture<Void> markDeleteAsync(String queueName, Position position);

    /**
     * Get mark delete position for a queue.
     * @param queueName name of the queue.
     * @return
     */
    CompletableFuture<Position> getMarkDeleteAsync(String queueName);

    /**
     * Add a queue {@link AmqpQueue} to the exchange.
     * @param queue AMQP queue.
     */
    CompletableFuture<Void> addQueue(AmqpQueue queue);

    /**
     * Get a queue {@link AmqpQueue} with queue name.
     * @param queueName AMQP queue name.
     * @return AMQP queue.
     */
    AmqpQueue getQueue(String queueName);

    /**
     * Remove a queue {@link AmqpQueue} from the exchange.
     * @param queue AMQP queue.
     */
    void removeQueue(AmqpQueue queue);

    int getQueueSize();

    /**
     * This method is used by destination exchange;
     * @param sourceEx
     * @param routingKey
     * @param params
     * @return
     */
    CompletableFuture<Void> bindExchange(AmqpExchange sourceEx, String routingKey, Map<String, Object> params);

    /**
     * This method is used by destination exchange;
     * @param sourceEx
     * @param routingKey
     * @param params
     */
    CompletableFuture<Void> unbindExchange(AmqpExchange sourceEx, String routingKey, Map<String, Object> params);

    /**
     * This method is used by source exchange;
     * @param destinationEx
     * @param routingKey
     * @param params
     */
    CompletableFuture<Void> addExchange(AmqpExchange destinationEx, String routingKey,
                                        Map<String, Object> params);

    /**
     * This method is used by source exchange;
     * @param destinationEx
     * @param routingKey
     * @param params
     */
    void removeExchange(AmqpExchange destinationEx, String routingKey, Map<String, Object> params);

    int getExchangeSize();

    AmqpMessageRouter getRouter(String sourceEx);

}
