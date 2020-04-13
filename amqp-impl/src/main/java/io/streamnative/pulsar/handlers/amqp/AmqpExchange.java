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
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.Message;

/**
 * Interface of the AMQP exchange.
 * The AMQP broker should maintaining exchanges in a Map, so that the broker can find the right exchange
 * to write messages and read messages.
 */
public interface AmqpExchange {

    /**
     * Exchange type, corresponding to AMQP protocol.
     *
     */
    enum Type{
        Direct,
        Fanout,
        Topic,
        Headers;

        public static Type value(String type) {
            if (type == null || type.length() == 0) {
                return null;
            }
            switch (type) {
                case "direct":
                    return Direct;
                case "fanout":
                    return Fanout;
                case "topic":
                    return Topic;
                case "headers":
                    return Headers;
                default:
                    return null;
            }
        }

    }

    /**
     * Get the name of the exchange.
     * The exchange name is the identify of an exchange.
     * @return name of the exchange.
     */
    String getName();

    boolean getDurable();

    /**
     * Get the type {@link Type} of the exchange.
     * @return the type of the exchange.
     */
    AmqpExchange.Type getType();

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
    void addQueue(AmqpQueue queue);

    /**
     * Remove a queue {@link AmqpQueue} from the exchange.
     * @param queue AMQP queue.
     */
    void removeQueue(AmqpQueue queue);

}
