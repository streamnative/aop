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
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.getExchangeType;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isBuildInExchange;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isDefaultExchange;

import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;

/**
 * Logic of queue.
 */
@Slf4j
public class QueueServiceImpl implements QueueService {
    private ExchangeContainer exchangeContainer;
    private QueueContainer queueContainer;

    public QueueServiceImpl(ExchangeContainer exchangeContainer,
                            QueueContainer queueContainer) {
        this.exchangeContainer = exchangeContainer;
        this.queueContainer = queueContainer;
    }

    @Override
    public CompletableFuture<AmqpQueue> queueDeclare(NamespaceName namespaceName, String queue, boolean passive,
                                                     boolean durable, boolean exclusive, boolean autoDelete,
                                                     boolean nowait, FieldTable arguments, long connectionId) {
        final AMQShortString finalQueue;
        if ((queue == null) || (queue.length() == 0)) {
            finalQueue = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
        } else {
            finalQueue = AMQShortString.createAMQShortString(queue);
        }
        CompletableFuture<AmqpQueue> future = new CompletableFuture<>();
        getQueue(namespaceName, finalQueue.toString(), !passive, connectionId)
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
                    queueContainer.deleteQueue(namespaceName, amqpQueue.getName());
                    // TODO delete the binding with the default exchange and delete the topic in pulsar.
                    future.complete(null);
                }
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> queueBind(NamespaceName namespaceName, String queue, String exchange, String bindingKey,
                          boolean nowait, FieldTable argumentsTable, long connectionId) {
//        int channelId = channel.getChannelId();
//        AmqpConnection connection = channel.getConnection();
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
//        Map<String, Object> arguments = FieldTable.convertToMap(argumentsTable);
//        if (queue == null || StringUtils.isEmpty(queue.toString())) {
//            AmqpQueue amqpQueue = channel.getDefaultQueue();
//            if (amqpQueue != null && bindingKey == null) {
//                bindingKey = AMQShortString.valueOf(amqpQueue.getName());
//            }
//            bind(channel, exchange, amqpQueue, bindingKey.toString(), arguments);
//        } else {
//            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
//                    queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
//            AMQShortString finalBindingKey = bindingKey;
//            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
//                if (throwable != null) {
//                    log.error("Failed to get topic from queue container", throwable);
//                    channel.closeChannel(ErrorCodes.INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
//                } else {
//                    if (amqpQueue == null) {
//                        channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
//                        return;
//                    }
//                    channel.checkExclusiveQueue(amqpQueue);
//                    if (null == finalBindingKey) {
//                        bind(channel, exchange, amqpQueue, amqpQueue.getName(), arguments);
//                    } else {
//                        bind(channel, exchange, amqpQueue, finalBindingKey.toString(), arguments);
//                    }
//                }
//            });
//        }
    }

    @Override
    public void queueUnbind(AmqpChannel channel, AMQShortString queue, AMQShortString exchange,
                            AMQShortString bindingKey, FieldTable arguments, long connectionId) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                    exchange, bindingKey, arguments);
        }
        getQueue(connection.getNamespaceName(), queue.toString(),  false, connectionId)
                .whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                channel.closeChannel(ErrorCodes.INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                    return;
                }
                String exchangeName;
                if (channel.isDefaultExchange(exchange)) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = exchange.toString();
                }
                CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                        exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName, false, null);
                amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable1) -> {
                    if (throwable1 != null) {
                        log.error("Failed to get topic from exchange container", throwable1);
                        channel.closeChannel(ErrorCodes.INTERNAL_ERROR, "Failed to get exchange: " + throwable1.getMessage());
                    } else {
                        try {
                            amqpQueue.unbindExchange(amqpExchange);
                            if (amqpExchange.getAutoDelete() && (amqpExchange.getQueueSize() == 0)) {
                                exchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                                amqpExchange.getTopic().delete().get();
                            }
                            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
                            connection.writeFrame(responseBody.generateFrame(channelId));
                        } catch (Exception e) {
                            connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                                    "unbind failed:" + e.getMessage(), channelId);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void queuePurge(AmqpChannel channel, AMQShortString queue, boolean nowait, long connectionId) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueuePurge[ queue: {}, nowait:{} ]", channelId, queue, nowait);
        }
        // TODO queue purge process

        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(0);
        connection.writeFrame(responseBody.generateFrame(channelId));
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
//                MethodRegistry methodRegistry = connection.getMethodRegistry();
//                AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
//                connection.writeFrame(responseBody.generateFrame(channelId));
            } catch (Exception e) {
                log.warn("Failed to bind queue[{}] with exchange[{}].", amqpQueue.getName(), exchange, e);
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR,
                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", false, true));
//                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
//                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
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

}
