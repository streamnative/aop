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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.api.proto.KeyValue;

/**
 * Message router is used to replicate message IDs from exchange to queue.
 */
public interface AmqpMessageRouter {

    /**
     * Get the type of this message router.
     *
     * @return {@link ExchangeType} type of this message router
     */
    ExchangeType getType();

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

    void setDestinationExchange(AmqpExchange exchange);

    AmqpExchange getDestinationExchange();

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
     * bindingKey is the value when create bind.
     *
     * @param binding bindingKey
     */
    void addBinding(AmqpBinding binding);

    /**
     * bindingKeys is the value when create bind.
     *
     * @param bindings bindingKeys
     */
    void setBindings(Map<String, AmqpBinding> bindings);

    /**
     * Get binding keys.
     *
     * @return list of bindingKeys.
     */
    Map<String, AmqpBinding> getBindings();

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

    CompletableFuture<Void> routingMessageToEx(ByteBuf payload, String routingKey, List<KeyValue> messageKeyValues,
                                               Map<String, Object> properties);

}
