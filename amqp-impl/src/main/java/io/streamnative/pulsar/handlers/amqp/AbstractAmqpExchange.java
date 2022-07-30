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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class of AMQP exchange.
 */
public abstract class AbstractAmqpExchange implements AmqpExchange {

    protected final String exchangeName;
    protected final ExchangeType exchangeType;
    protected Set<AmqpQueue> queues;
    protected boolean durable;
    protected boolean autoDelete;
    protected boolean internal;
    protected Map<String, Object> arguments;
    protected Map<String, Set<AmqpQueue>> bindingKeyQueueMap;

    public static final String DEFAULT_EXCHANGE_DURABLE = "aop.direct.durable";

    protected Set<AmqpExchange> exchanges;
    protected Map<String, Set<AmqpExchange>> bindingKeyExchangeMap;
    protected Map<String, AmqpMessageRouter> routerMap = new ConcurrentHashMap<>();

    protected AbstractAmqpExchange(String exchangeName, ExchangeType exchangeType,
                                   Set<AmqpQueue> queues, Set<AmqpExchange> exchanges, boolean durable,
                                   boolean autoDelete, boolean internal,
                                   Map<String, Object> arguments) {
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.queues = queues;
        this.exchanges = exchanges;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
        this.arguments = arguments;
        if (this.exchangeType == ExchangeType.DIRECT) {
            bindingKeyQueueMap = new ConcurrentHashMap<>();
            bindingKeyExchangeMap = new ConcurrentHashMap<>();
        }
    }

    @Override
    public CompletableFuture<Void> addQueue(AmqpQueue queue) {
        queues.add(queue);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public AmqpQueue getQueue(String queueName) {
        AmqpQueue queue = null;
        for (AmqpQueue q : queues) {
            if (q.getName().equals(queueName)) {
                queue = q;
            }
        }
        return queue;
    }

    @Override
    public void removeQueue(AmqpQueue queue) {
        queues.remove(queue);
    }

    @Override
    public int getQueueSize() {
        return queues.size();
    }

    @Override
    public String getName() {
        return exchangeName;
    }

    @Override
    public boolean getDurable() {
        return durable;
    }

    @Override
    public boolean getAutoDelete() {
        return autoDelete;
    }

    @Override
    public boolean getInternal() {
        return internal;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    @Override
    public ExchangeType getType() {
        return exchangeType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractAmqpExchange that = (AbstractAmqpExchange) o;
        return exchangeName.equals(that.exchangeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeName);
    }
}
