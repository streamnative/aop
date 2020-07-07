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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Message router is used to replicate message IDs from exchange to queue.
 */
public interface AmqpMessageRouter {

    /**
     * Message router type.
     * Direct message router is used to bind {@link AmqpExchange.Type#Direct} exchange.
     * Fanout message router is used to bind {@link AmqpExchange.Type#Fanout} exchange.
     * Topic message router is used to bind {@link AmqpExchange.Type#Topic} exchange.
     * Headers message router is used to bind {@link AmqpExchange.Type#Headers} exchange.
     */
    enum Type {
        Direct,
        Fanout,
        Topic,
        Headers;
    }

    /**
     * Get the type of this message router.
     *
     * @return {@link Type} type of this message router
     */
    Type getType();

    /**
     * Set AMQP exchange {@link AmqpExchange} for the message router.
     * @param exchange {@link AmqpExchange}
     */
    void setExchange(AmqpExchange exchange);

    /**
     * Get the exchange {@link AmqpExchange} of the message router.
     *
     * @return the {@link AmqpExchange} exchange of the message router
     */
    AmqpExchange getExchange();

    /**
     * Set AMQP queue {@link AmqpQueue} for the message router.
     * @param queue {@link AmqpQueue}
     */
    void setQueue(AmqpQueue queue);

    /**
     * Get the queue {@link AmqpQueue} of the message router.
     *
     * @return the {@link AmqpQueue} exchange of the message router
     */
    AmqpQueue getQueue();

    /**
     * bindingKey is the value when create bind.
     *
     * @param bindingKey bindingKey
     */
    void addBindingKey(String bindingKey);

    /**
     * bindingKeys is the value when create bind.
     *
     * @param bindingKeys bindingKeys
     */
    void setBindingKeys(Set<String> bindingKeys);

    /**
     * Get binding keys.
     *
     * @return list of bindingKeys.
     */
    Set<String> getBindingKey();

    /**
     * header properties.
     *
     * @param arguments header properties
     */
    void setArguments(Map<String, Object> arguments);

    /**
     * get header properties.
     *
     * @return map of properties
     */
    Map<String, Object> getArguments();

    /**
     * Routes the message ID to the queue.
     *
     * @param ledgerId   ledger ID
     * @param entryId    entry ID
     * @param routingKey routingKey
     * @param properties header properties
     */
    CompletableFuture<Void> routingMessage(long ledgerId, long entryId, String routingKey,
                                           Map<String, Object> properties);

}
