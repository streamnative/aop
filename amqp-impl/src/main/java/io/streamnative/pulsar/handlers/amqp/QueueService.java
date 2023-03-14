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
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * Logic of queue.
 */
public interface QueueService {

    /**
     *  Declare a queue.
     *
     * @param queue the name of the queue
     * @param passive Declare a queue passively; i.e., check if it exists.  In AMQP
     *       0-9-1, all arguments aside from nowait are ignored; and sending
     *       nowait makes this method a no-op, so we default it to false.
     * @param durable true if we are declaring a durable queue (the exchange will survive a server restart)
     * @param exclusive true if we are declaring an exclusive queue (restricted to this connection)
     * @param autoDelete true if the server should delete the queue when it is no longer in use
     * @param nowait set true will return nothing (as there will be no response from the server)
     * @param arguments other properties (construction arguments) for the queue
     */
    CompletableFuture<AmqpQueue> queueDeclare(NamespaceName namespaceName, String queue, boolean passive,
                                              boolean durable, boolean exclusive, boolean autoDelete, boolean nowait,
                                              Map<String, Object> arguments, long connectionId);

    /**
     *  Delete a queue.
     *
     * @param queue the name of the queue
     * @param ifUnused true if the queue should be deleted only if not in use
     * @param ifEmpty true if the queue should be deleted only if empty
     */
    CompletableFuture<Void> queueDelete(NamespaceName namespaceName, String queue, boolean ifUnused, boolean ifEmpty,
                                        long connectionId);

    /**
     * Bind a queue to an exchange.
     *
     * @param queue the name of the queue
     * @param exchange the name of the exchange
     * @param bindingKey the key to use for the binding
     * @param nowait set true will return nothing (as there will be no response from the server)
     * @param argumentsTable other properties (binding parameters)
     */
    CompletableFuture<Void> queueBind(NamespaceName namespaceName, String queue, String exchange, String bindingKey,
                   boolean nowait, FieldTable argumentsTable, long connectionId);

    /**
     * Unbinds a queue from an exchange.
     *
     * @param queue the name of the queue
     * @param exchange the name of the exchange
     * @param bindingKey the key to use for the binding
     * @param arguments other properties (binding parameters)
     */
    CompletableFuture<Void> queueUnbind(NamespaceName namespaceName, String queue, String exchange, String bindingKey,
                     FieldTable arguments, long connectionId);

    /**
     * Purges the contents of the given queue.
     *
     * @param queue the name of the queue
     * @param nowait set true will return nothing (as there will be no response from the server)
     */
    CompletableFuture<Void> queuePurge(NamespaceName namespaceName, String queue, boolean nowait, long connectionId);

    CompletableFuture<AmqpQueue> getQueue(NamespaceName namespaceName, String queue, boolean createIfMissing,
                                          long connectionId);
    CompletableFuture<AmqpQueue> getQueue(NamespaceName namespaceName, String queue, boolean createIfMissing,
                                          long connectionId,  boolean durable, boolean exclusive, boolean autoDelete,
                                          boolean nowait, Map<String, Object> arguments);
}
