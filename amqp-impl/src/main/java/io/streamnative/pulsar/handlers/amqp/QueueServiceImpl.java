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

import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;

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
    public void queueDeclare(AmqpChannel channel, AMQShortString queue, boolean passive, boolean durable,
                             boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                            + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        if ((queue == null) || (queue.length() == 0)) {
            queue = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
        }
        AMQShortString finalQueue = queue;
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueContainer.asyncGetQueue(connection.getNamespaceName(), finalQueue.toString(), !passive);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
            } else {
                if (null == amqpQueue) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + finalQueue);
                } else {
                    channel.checkExclusiveQueue(amqpQueue);
                    channel.setDefaultQueue(amqpQueue);

                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(finalQueue, 0, 0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            }
        });
    }

    @Override
    public void queueDelete(AmqpChannel channel, AMQShortString queue, boolean ifUnused,
                            boolean ifEmpty, boolean nowait) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                    ifUnused, ifEmpty, nowait);
        }
        if ((queue == null) || (queue.length() == 0)) {
            //get the default queue on the channel:
            AmqpQueue amqpQueue = channel.getDefaultQueue();
            delete(channel, amqpQueue);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                    queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to get topic from queue container", throwable);
                    channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
                } else {
                    if (null == amqpQueue) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queue.toString());
                    } else {
                        delete(channel, amqpQueue);
                    }
                }
            });
        }
    }

    @Override
    public void queueBind(AmqpChannel channel, AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                          boolean nowait, FieldTable argumentsTable) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        Map<String, Object> arguments = FieldTable.convertToMap(argumentsTable);
        if (queue == null || StringUtils.isEmpty(queue.toString())) {
            AmqpQueue amqpQueue = channel.getDefaultQueue();
            if (amqpQueue != null && bindingKey == null) {
                bindingKey = AMQShortString.valueOf(amqpQueue.getName());
            }
            bind(channel, exchange, amqpQueue, bindingKey.toString(), arguments);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                    queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            AMQShortString finalBindingKey = bindingKey;
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to get topic from queue container", throwable);
                    channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
                } else {
                    if (amqpQueue == null) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                        return;
                    }
                    channel.checkExclusiveQueue(amqpQueue);
                    if (null == finalBindingKey) {
                        bind(channel, exchange, amqpQueue, amqpQueue.getName(), arguments);
                    } else {
                        bind(channel, exchange, amqpQueue, finalBindingKey.toString(), arguments);
                    }
                }
            });
        }
    }

    @Override
    public void queueUnbind(AmqpChannel channel, AMQShortString queue, AMQShortString exchange,
                            AMQShortString bindingKey, FieldTable arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                    exchange, bindingKey, arguments);
        }
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from queue container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get queue: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                    return;
                }
                channel.checkExclusiveQueue(amqpQueue);
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
                        channel.closeChannel(INTERNAL_ERROR, "Failed to get exchange: " + throwable1.getMessage());
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
                            connection.sendConnectionClose(INTERNAL_ERROR,
                                    "unbind failed:" + e.getMessage(), channelId);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void queuePurge(AmqpChannel channel, AMQShortString queue, boolean nowait) {
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

    private void delete(AmqpChannel channel, AmqpQueue amqpQueue) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (amqpQueue == null) {
            channel.closeChannel(ErrorCodes.NOT_FOUND, "Queue does not exist.");
        } else {
            channel.checkExclusiveQueue(amqpQueue);
            queueContainer.deleteQueue(connection.getNamespaceName(), amqpQueue.getName());
            // TODO delete the binding with the default exchange and delete the topic in pulsar.

            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        }
    }

    private void bind(AmqpChannel channel, AMQShortString exchange, AmqpQueue amqpQueue,
                      String bindingKey, Map<String, Object> arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        String exchangeName = channel.isDefaultExchange(exchange)
                ? AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE : exchange.toString();
        if (exchangeName.equals(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)) {
            channel.closeChannel(ErrorCodes.ACCESS_REFUSED, "Can not bind to default exchange ");
        }
        String exchangeType = null;
        boolean createIfMissing = false;
        if (channel.isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = channel.getExchangeType(exchange.toString());
        }

        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                exchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName,
                        createIfMissing, exchangeType);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic from exchange container", throwable);
                channel.closeChannel(INTERNAL_ERROR, "Failed to get exchange: " + throwable.getMessage());
            } else {
                AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(amqpExchange.getType());
                if (messageRouter == null) {
                    connection.sendConnectionClose(INTERNAL_ERROR, "Unsupported router type!", channelId);
                    return;
                }
                try {
                    amqpQueue.bindExchange(amqpExchange, messageRouter,
                            bindingKey, arguments);
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                    connection.writeFrame(responseBody.generateFrame(channelId));
                } catch (Exception e) {
                    log.warn("Failed to bind queue[{}] with exchange[{}].", amqpQueue.getName(), exchange, e);
                    connection.sendConnectionClose(INTERNAL_ERROR,
                            "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
                }
            }
        });
    }

}
