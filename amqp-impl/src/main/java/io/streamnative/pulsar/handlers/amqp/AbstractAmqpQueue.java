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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for AMQP queue.
 */
public abstract class AbstractAmqpQueue implements AmqpQueue {

    protected final String queueName;
    protected final boolean durable;
    protected final Map<String, AmqpMessageRouter> routers = new ConcurrentHashMap<>();

    protected AbstractAmqpQueue(String queueName, boolean durable) {
        this.queueName = queueName;
        this.durable = durable;
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
    public AmqpMessageRouter getRouter(String exchangeName) {
        return routers.get(exchangeName);
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
    public void bindExchange(AmqpExchange exchange, AmqpMessageRouter router, String bindingKey,
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
        exchange.addQueue(this);
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
}
