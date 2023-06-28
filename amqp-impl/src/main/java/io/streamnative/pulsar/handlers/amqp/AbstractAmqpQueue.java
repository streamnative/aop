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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

/**
 * Base class for AMQP queue.
 */
public abstract class AbstractAmqpQueue implements AmqpQueue {

    protected final String queueName;
    protected final boolean durable;
    // which connection create the queue.
    protected final long connectionId;
    protected boolean exclusive;
    protected boolean autoDelete;
    protected final Map<String, AmqpMessageRouter> routers = new ConcurrentHashMap<>();
    protected final Map<String, Object> arguments = new HashMap<>();
    @Getter
    protected Map<String, String> properties;

    protected AbstractAmqpQueue(String queueName, boolean durable, long connectionId) {
        this.queueName = queueName;
        this.durable = durable;
        this.connectionId = connectionId;
        this.autoDelete = false;
        this.exclusive = false;
    }

    protected AbstractAmqpQueue(String queueName,
                                boolean durable, long connectionId,
                                boolean exclusive, boolean autoDelete, Map<String, String> properties) {
        this.queueName = queueName;
        this.durable = durable;
        this.connectionId = connectionId;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
        this.properties = properties;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public boolean getDurable() {
        return durable;
    }

    @Override
    public boolean getExclusive() {
        return exclusive;
    }
    @Override
    public boolean getAutoDelete() {
        return autoDelete;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public AmqpMessageRouter getRouter(String exchangeName) {
        return routers.get(exchangeName);
    }

    @Override
    public Collection<AmqpMessageRouter> getRouters() {
        return routers.values();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractAmqpQueue that = (AbstractAmqpQueue) o;
        return queueName.equals(that.queueName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueName);
    }

    @Override
    public CompletableFuture<Void> bindExchange(AmqpExchange exchange, AmqpMessageRouter router, String bindingKey,
                                                Map<String, Object> arguments) {
        // The same exchange and queue can have more than one binding,
        // if router had been created, get it and add a new bindingKey.
        if (isRouterExisted(exchange)) {
            this.routers.get(exchange.getName()).addBindingKey(bindingKey);
        } else {
            router.setExchange(exchange);
            router.setQueue(this);
            router.addBindingKey(bindingKey);
            router.setArguments(arguments);
            this.routers.put(router.getExchange().getName(), router);
        }
        return exchange.addQueue(this);
    }

    @Override
    public void unbindExchange(AmqpExchange exchange) {
        exchange.removeQueue(this);
        this.routers.remove(exchange.getName());
    }

    private boolean isRouterExisted(AmqpExchange exchange) {
        if (null != routers.get(exchange.getName())) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public long getConnectionId() {
        return connectionId;
    }

    @Override
    public boolean isExclusive() {
        return exclusive;
    }

    @Override
    public boolean isAutoDelete() {
        return autoDelete;
    }

    @Override
    public void close() {

    }
}
