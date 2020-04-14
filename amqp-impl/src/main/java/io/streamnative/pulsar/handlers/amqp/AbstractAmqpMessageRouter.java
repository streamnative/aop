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

import io.streamnative.pulsar.handlers.amqp.impl.DirectMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.FanoutMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.TopicMessageRouter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Base class for AMQP message router.
 */
public abstract class AbstractAmqpMessageRouter implements AmqpMessageRouter {

    protected AmqpExchange exchange;
    protected AmqpQueue queue;
    protected final AmqpMessageRouter.Type routerType;
    protected Set<String> bindingKeys;
    protected Map<String, Object> arguments;

    protected AbstractAmqpMessageRouter(Type routerType) {
        this.routerType = routerType;
        this.bindingKeys = new HashSet<>();
    }

    @Override
    public Type getType() {
        return routerType;
    }

    @Override
    public void setExchange(AmqpExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public AmqpExchange getExchange() {
        return exchange;
    }

    @Override
    public void setQueue(AmqpQueue queue) {
        this.queue = queue;
    }

    @Override
    public AmqpQueue getQueue() {
        return queue;
    }

    @Override
    public void addBindingKey(String bindingKey) {
        this.bindingKeys.add(bindingKey);
    }

    @Override
    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public static AmqpMessageRouter generateRouter(AmqpExchange.Type type) {

        if (type == null) {
            return null;
        }

        switch (type) {
            case Direct:
                return new DirectMessageRouter();
            case Fanout:
                return new FanoutMessageRouter();
            case Topic:
                return new TopicMessageRouter();
            case Headers:
                return new HeadersMessageRouter();
            default:
                return null;
        }
    }

}
