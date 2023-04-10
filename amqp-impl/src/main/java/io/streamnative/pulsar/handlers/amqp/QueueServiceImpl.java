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

import static io.streamnative.pulsar.handlers.amqp.utils.ExceptionUtils.getAoPException;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.JSON_MAPPER;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.getExchangeType;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isBuildInExchange;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isDefaultExchange;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * Logic of queue.
 */
@Slf4j
public class QueueServiceImpl implements QueueService {
    private ExchangeContainer exchangeContainer;
    private QueueContainer queueContainer;
    private AmqpTopicManager amqpTopicManager;

    public QueueServiceImpl(ExchangeContainer exchangeContainer,
                            QueueContainer queueContainer, AmqpTopicManager amqpTopicManager) {
        this.exchangeContainer = exchangeContainer;
        this.queueContainer = queueContainer;
        this.amqpTopicManager = amqpTopicManager;
    }

    @Override
    public CompletableFuture<AmqpQueue> queueDeclare(NamespaceName namespaceName, String queue, boolean passive,
                                                     boolean durable, boolean exclusive, boolean autoDelete,
                                                     boolean nowait, Map<String, Object> arguments, long connectionId) {
        final AMQShortString finalQueue;
        if ((queue == null) || (queue.length() == 0)) {
            finalQueue = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
        } else {
            finalQueue = AMQShortString.createAMQShortString(queue);
        }
        CompletableFuture<AmqpQueue> future = new CompletableFuture<>();
        getQueue(namespaceName, finalQueue.toString(), passive, connectionId,
                durable, exclusive, autoDelete, nowait, arguments)
                .whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                future.completeExceptionally(getAoPException(throwable, "Failed to get queue: " + finalQueue + ", "
                        + throwable.getMessage(), true, false));
            } else {
                if (null == amqpQueue) {
                    future.completeExceptionally(
                            new AoPException(ErrorCodes.NOT_FOUND, "No such queue: " + finalQueue, true, false));
                } else {
                    future.complete(amqpQueue);
                }
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> queueDelete(NamespaceName namespaceName, String queue,
                                               boolean ifUnused, boolean ifEmpty, long connectionId) {
        if ((queue == null) || (queue.length() == 0)) {
            return FutureUtil.failedFuture(new AoPException(ErrorCodes.ARGUMENT_INVALID,
                    "[QueueDelete] The queue name is empty.", true, false));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        getQueue(namespaceName, queue, false, connectionId)
                .whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                future.completeExceptionally(getAoPException(throwable, "Failed to get queue: "
                        + queue + ", " + throwable.getMessage(), true, false));
            } else {
                if (null == amqpQueue) {
                    future.completeExceptionally(
                            new AoPException(ErrorCodes.NOT_FOUND, "No such queue: " + queue, true, false));
                } else {
                    Topic topic = amqpQueue.getTopic();
                    ConcurrentOpenHashMap<String, ? extends Subscription> subscriptions = topic.getSubscriptions();
                    if (subscriptions != null) {
                        Subscription subscription = subscriptions.get(PersistentQueue.DEFAULT_SUBSCRIPTION);
                        if (subscription != null) {
                            if (ifUnused && CollectionUtils.isNotEmpty(subscription.getConsumers())) {
                                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, "Failed to delete queue: "
                                                + queue + ", Queue has active consumers", true, false));
                                return;
                            }
                            if (ifEmpty && subscription.getNumberOfEntriesInBacklog(false) > 0) {
                                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, "Failed to delete queue: "
                                        + queue + ", Queue has message", true, false));
                                return;
                            }
                        }
                    }
                    Collection<AmqpMessageRouter> routers = amqpQueue.getRouters();
                    if (!CollectionUtils.isEmpty(routers)) {
                        for (AmqpMessageRouter router : routers) {
                            // TODO need to change to async way
                            amqpQueue.unbindExchange(router.getExchange());
                        }
                    }
                    amqpQueue.close();
                    amqpQueue.getTopic().deleteForcefully().thenAccept(__ -> {
                        queueContainer.deleteQueue(namespaceName, amqpQueue.getName());
                        future.complete(null);
                    }).exceptionally(t -> {
                        future.completeExceptionally(t);
                        return null;
                    });
                }
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> queueBind(NamespaceName namespaceName, String queue, String exchange,
                                             String bindingKey, boolean nowait, FieldTable argumentsTable,
                                             long connectionId) {
        if (StringUtils.isEmpty(queue)) {
            return FutureUtil.failedFuture(new AoPException(ErrorCodes.ARGUMENT_INVALID,
                    "[QueueBind] The queue name is empty.", true, false));
        }

        String finalBindingKey;
        if (bindingKey == null) {
            finalBindingKey = queue;
        } else {
            finalBindingKey = bindingKey;
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        getQueue(namespaceName, queue, false, connectionId)
                .whenComplete(((amqpQueue, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to get topic {} from queue container", queue, throwable);
                        future.completeExceptionally(getAoPException(throwable, "Failed to get queue: "
                                + queue + ", " + throwable.getMessage(), true, false));
                        return;
                    }
                    if (amqpQueue == null) {
                        future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND, "No such queue: '"
                                + queue + "'", true, false));
                        return;
                    }
                    bind(namespaceName, exchange, amqpQueue, finalBindingKey, null).thenAccept(__ -> {
                        future.complete(null);
                    }).exceptionally(t -> {
                        future.completeExceptionally(t);
                        return null;
                    });
                }));
        return future;
    }

    @Override
    public CompletableFuture<Void> queueUnbind(NamespaceName namespaceName, String queue, String exchange,
                                               String bindingKey, FieldTable arguments, long connectionId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        getQueue(namespaceName, queue,  false, connectionId)
                .whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                future.completeExceptionally(getAoPException(throwable,
                        "Failed to get queue: " + throwable.getMessage(), true, false));
            } else {
                if (amqpQueue == null) {
                    future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND,
                            "No such queue: '" + queue + "'", true, false));
                    return;
                }
                String exchangeName;
                if (isDefaultExchange(exchange)) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = exchange;
                }
                CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                        exchangeContainer.asyncGetExchange(namespaceName, exchangeName, false, null);
                amqpExchangeCompletableFuture.whenComplete((amqpExchange, exThrowable) -> {
                    if (exThrowable != null) {
                        log.error("Failed to get topic from exchange container", exThrowable);
                        future.completeExceptionally(getAoPException(exThrowable,
                                "Failed to get exchange: " + exThrowable.getMessage(), true, false));
                    } else {
                        try {
                            amqpQueue.unbindExchange(amqpExchange);
                            if (amqpExchange.getAutoDelete() && (amqpExchange.getQueueSize() == 0)) {
                                exchangeContainer.deleteExchange(namespaceName, exchangeName);
                                amqpExchange.getTopic().delete().get();
                            }
                            future.complete(null);
                        } catch (Exception e) {
                            future.completeExceptionally(getAoPException(e,
                                    "Unbind failed:" + e.getMessage(), false, true));
                        }
                    }
                });
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> queuePurge(NamespaceName namespaceName, String queue, boolean nowait,
                                              long connectionId) {
        // TODO queue purge process
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> bind(NamespaceName namespaceName, String exchange, AmqpQueue amqpQueue,
                      String bindingKey, Map<String, Object> arguments) {
        String exchangeName = isDefaultExchange(exchange)
                ? AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE : exchange;
        if (exchangeName.equals(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)) {
            return FutureUtil.failedFuture(new AoPException(ErrorCodes.ACCESS_REFUSED,
                    "Can not bind to default exchange " + exchangeName, true, false));
        }
        String exchangeType = null;
        boolean createIfMissing = false;
        if (isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = getExchangeType(exchange);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        exchangeContainer.asyncGetExchange(namespaceName, exchangeName, createIfMissing, exchangeType)
                .whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from exchange container", throwable);
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, "Failed to get exchange: "
                        + throwable.getMessage(), true, false));
                return;
            }
            AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(amqpExchange.getType());
            if (messageRouter == null) {
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, "Unsupported router type!",
                        false, true));
                return;
            }
            try {
                amqpQueue.bindExchange(amqpExchange, messageRouter, bindingKey, arguments);
                future.complete(null);
            } catch (Exception e) {
                log.warn("Failed to bind queue[{}] with exchange[{}].", amqpQueue.getName(), exchange, e);
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR,
                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", false, true));
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<AmqpQueue> getQueue(NamespaceName namespaceName, String queueName, boolean createIfMissing,
                                                 long connectionId) {
        CompletableFuture<AmqpQueue> future = new CompletableFuture<>();
        queueContainer.asyncGetQueue(namespaceName, queueName, createIfMissing).whenComplete((queue, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, "Failed to get queue "
                        + queueName + " in vhost " + namespaceName.getLocalName(), false, true));
                return;
            }
            if (queue != null && queue.isExclusive() && queue.getConnectionId() != connectionId) {
                future.completeExceptionally(new AoPException(ErrorCodes.ALREADY_EXISTS,
                        "cannot obtain exclusive access to locked queue '" + queue + "' in vhost '"
                                + namespaceName.getLocalName() + "'", false, true));
                return;
            }
            future.complete(queue);
        });
        return future;
    }
    @Override
    public CompletableFuture<AmqpQueue> getQueue(NamespaceName namespaceName, String queueName, boolean passive,
                                                 long connectionId,  boolean durable, boolean exclusive, boolean autoDelete,
                                                 boolean nowait, Map<String, Object> arguments) {
        CompletableFuture<AmqpQueue> future = new CompletableFuture<>();
        queueContainer.asyncGetQueue(namespaceName, queueName, passive, durable, exclusive, autoDelete, nowait,
                arguments).whenComplete((queue, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR, throwable.getMessage(), false, true));
                return;
            }
            if (queue != null && queue.isExclusive() && queue.getConnectionId() != connectionId) {
                future.completeExceptionally(new AoPException(ErrorCodes.ALREADY_EXISTS,
                        "cannot obtain exclusive access to locked queue '" + queue + "' in vhost '"
                                + namespaceName.getLocalName() + "'", false, true));
                return;
            }
            future.complete(queue);
        });
        return future;
    }

    /**
     * Query the list of exchanges bound to the queue
     *
     * @param namespaceName
     * @param queue
     * @param exchange
     * @param params
     * @return
     */
    @Override
    public CompletableFuture<Void> queueBind(NamespaceName namespaceName, String queue, String exchange,
                                           BindingParams params) {
        String topicName = PersistentQueue.getQueueTopicName(namespaceName, queue);
        return amqpTopicManager.getTopic(topicName, false, null)
                .thenCompose(topic -> {
                    if (topic == null) {
                        throw new AoPServiceRuntimeException.NoSuchQueueException("Queue [" + queue + "] not created");
                    }
                    PersistentTopic persistentTopic = (PersistentTopic) topic;
                    Set<PersistentExchange.Binding> bindings = Sets.newHashSet();
                    String bindingsJson;
                    try {
                        if (persistentTopic.getManagedLedger().getProperties().containsKey("BINDINGS")) {
                            List<PersistentExchange.Binding> amqpQueueProperties = JSON_MAPPER.readValue(
                                    persistentTopic.getManagedLedger().getProperties().get("BINDINGS"),
                                    new TypeReference<>() {
                                    });
                            bindings.addAll(amqpQueueProperties);
                        }
                        bindings.add(new PersistentExchange.Binding(queue, "queue", params.getRoutingKey(), exchange,
                                params.getArguments()));
                        bindingsJson = JSON_MAPPER.writeValueAsString(bindings);
                    } catch (JsonProcessingException e) {
                        log.error("Failed to bind queue {} to exchange {}", queue, exchange, e);
                        return FutureUtil.failedFuture(e);
                    }
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    persistentTopic.getManagedLedger().asyncSetProperty("BINDINGS", bindingsJson,
                            new AsyncCallbacks.UpdatePropertiesCallback() {
                                @Override
                                public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                                    future.complete(null);
                                }

                                @Override
                                public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                                    log.error("Failed to save binding metadata for bind operation.", exception);
                                    future.completeExceptionally(exception);
                                }
                            }, null);
                    return future;
                });
    }

    @Override
    public CompletableFuture<Void> queueUnBind(NamespaceName namespaceName, String queue, String exchange,
                                           String propsKey) {
        String topicName = PersistentQueue.getQueueTopicName(namespaceName, queue);
        return amqpTopicManager.getTopic(topicName, false, null)
                .thenCompose(topic -> {
                    if (topic == null) {
                        throw new AoPServiceRuntimeException.NoSuchQueueException("Queue [" + queue + "] not created");
                    }
                    PersistentTopic persistentTopic = (PersistentTopic) topic;
                    Set<PersistentExchange.Binding> bindings = Sets.newHashSet();
                    String bindingsJson;
                    try {
                        if (persistentTopic.getManagedLedger().getProperties().containsKey("BINDINGS")) {
                            List<PersistentExchange.Binding> amqpQueueProperties = JSON_MAPPER.readValue(
                                    persistentTopic.getManagedLedger().getProperties().get("BINDINGS"),
                                    new TypeReference<>() {
                                    });
                            bindings.addAll(amqpQueueProperties);
                        }
                        bindings.removeIf(binding -> exchange.equals(binding.getSource())
                                && queue.equals(binding.getDes())
                                && propsKey.equals(binding.getKey()));
                        bindingsJson = JSON_MAPPER.writeValueAsString(bindings);
                    } catch (JsonProcessingException e) {
                        log.error("Failed to bind queue {} to exchange {}", queue, exchange, e);
                        return FutureUtil.failedFuture(e);
                    }
                    CompletableFuture<Void> future = new CompletableFuture<>();
                    persistentTopic.getManagedLedger().asyncSetProperty("BINDINGS", bindingsJson,
                            new AsyncCallbacks.UpdatePropertiesCallback() {
                                @Override
                                public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                                    future.complete(null);
                                }

                                @Override
                                public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                                    log.error("Failed to save binding metadata for bind operation.", exception);
                                    future.completeExceptionally(exception);
                                }
                            }, null);
                    return future;
                }).exceptionally(throwable -> {
                    log.error("Failed to save binding metadata for bind operation.", throwable);
                    return null;
                });
    }

}
