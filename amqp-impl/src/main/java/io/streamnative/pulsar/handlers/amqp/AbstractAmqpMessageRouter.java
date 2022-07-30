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

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.impl.DirectMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.FanoutMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.TopicMessageRouter;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Base class for AMQP message router.
 */
@Slf4j
public abstract class AbstractAmqpMessageRouter implements AmqpMessageRouter {

    protected AmqpExchange exchange;
    protected AmqpExchange destinationEx;
    protected AmqpQueue queue;
    protected final AmqpMessageRouter.Type routerType;
    protected Set<String> bindingKeys;
    protected Set<AmqpBinding> bindings = Sets.newConcurrentHashSet();
    protected Map<String, Object> arguments;

    protected AbstractAmqpMessageRouter(Type routerType) {
        this.routerType = routerType;
        this.bindingKeys = Sets.newConcurrentHashSet();
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
    public void setDestinationExchange(AmqpExchange exchange) {
        this.destinationEx = exchange;
    }

    @Override
    public AmqpExchange getDestinationExchange() {
        return destinationEx;
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
    public void setBindingKeys(Set<String> bindingKeys) {
        this.bindingKeys = bindingKeys;
    }

    @Override
    public Set<String> getBindingKey() {
        return bindingKeys;
    }

    @Override
    public void addBinding(AmqpBinding binding) {
        this.bindings.add(binding);
    }

    @Override
    public void setBindings(Set<AmqpBinding> bindings) {
        this.bindings = bindings;
    }

    @Override
    public Set<AmqpBinding> getBindings() {
        return bindings;
    }

    @Override
    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    public static AmqpMessageRouter generateRouter(ExchangeType type) {

        if (type == null) {
            return null;
        }

        switch (type) {
            case DIRECT:
                return new DirectMessageRouter();
            case FANOUT:
                return new FanoutMessageRouter();
            case TOPIC:
                return new TopicMessageRouter();
            case HEADERS:
                return new HeadersMessageRouter();
            default:
                return null;
        }
    }

    @Override
    public CompletableFuture<Void> routingMessage(long ledgerId, long entryId,
                                                  String routingKey, Map<String, Object> properties) {
        if (isMatch(properties)) {
            try {
                return queue.writeIndexMessageAsync(exchange.getName(), ledgerId, entryId, properties);
            } catch (Exception e) {
                log.error("Failed to route message from exchange {} to queue {} for pos {}:{}.",
                        exchange.getName(), queue.getName(), ledgerId, entryId, e);
                return FutureUtil.failedFuture(e);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> routingMessageToEx(ByteBuf payload, String routingKey, Map<String, Object> properties) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (isMatch(properties)) {
            return destinationEx.writeMessageAsync(MessageConvertUtils.entryToMessage(payload, properties), routingKey)
                    .thenApply(__ -> null);
//            try {
//                ((PersistentTopic) destinationEx.getTopic()).getManagedLedger().asyncAddEntry(entry.getDataBuffer(), new AsyncCallbacks.AddEntryCallback() {
//                    @Override
//                    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
//                        future.complete(null);
//                    }
//
//                    @Override
//                    public void addFailed(ManagedLedgerException exception, Object ctx) {
//                        log.error("Failed to write message to destination exchange {}",
//                                destinationEx.getName(), exception);
//                        future.completeExceptionally(exception);
//                    }
//                }, null);
//            } catch (Exception e) {
//                log.error("Failed to route message to exchange.", e);
//                return FutureUtil.failedFuture(e);
//            }
        } else {
            future.complete(null);
        }
        return future;
    }

    public abstract boolean isMatch(Map<String, Object> properties);

}
