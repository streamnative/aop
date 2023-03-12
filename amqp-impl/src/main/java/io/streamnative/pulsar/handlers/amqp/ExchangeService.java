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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Logic of exchange.
 */
public interface ExchangeService {

    /**
     * Declare a exchange.
     *
     * @param exchange the name of the exchange
     * @param type the exchange type
     * @param passive Declare a queue exchange; i.e., check if it exists. In AMQP
     *             0-9-1, all arguments aside from nowait are ignored; and sending
     *             nowait makes this method a no-op, so we default it to false.
     * @param durable true if we are declaring a durable exchange (the exchange will survive a server restart)
     * @param autoDelete true if the server should delete the exchange when it is no longer in use
     * @param internal true if the exchange is internal, i.e. can't be directly published to by a client
     * @param arguments other properties (construction arguments) for the exchange
     */
    CompletableFuture<AmqpExchange> exchangeDeclare(NamespaceName namespaceName, String exchange, String type,
                                                    boolean passive, boolean durable, boolean autoDelete,
                                                    boolean internal, Map<String, Object> arguments);

    /**
     * Delete a exchange.
     *
     * @param exchange the name of the exchange
     * @param ifUnused true to indicate that the exchange is only to be deleted if it is unused
     */
    CompletableFuture<Void> exchangeDelete(NamespaceName namespaceName, String exchange, boolean ifUnused);

    /**
     * Judge the exchange and the queue whether had bound.
     *
     * @param exchange the name of the exchange
     * @param routingKey the routing key to use for the binding
     * @param queueName the name of the queue
     */
    CompletableFuture<Integer> exchangeBound(NamespaceName namespaceName, String exchange, String routingKey,
                                          String queueName);

    /**
     * Exchange bind exchange.
     *
     * @param namespaceName namespace name of the vhost
     * @param destination destination exchange name
     * @param source source exchange name
     * @param routingKey routing key
     * @param params params
     * @return bind result
     */
    CompletableFuture<Void> exchangeBind(NamespaceName namespaceName, String destination, String source,
                                         String routingKey, Map<String, Object> params);

    /**
     * Exchange unbind exchange.
     *
     * @param namespaceName namespace name of the vhost
     * @param destination destination exchange name
     * @param source source exchange name
     * @param routingKey routing key
     * @param params params
     * @return unbind result
     */
    CompletableFuture<Void> exchangeUnbind(NamespaceName namespaceName, String destination, String source,
                                         String routingKey, Map<String, Object> params);
}
