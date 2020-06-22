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

@Slf4j
public class QueueServiceImpl implements QueueService {
    private final int channelId;
    private final AmqpConnection connection;
    private AmqpChannel amqpChannel;

    public QueueServiceImpl(AmqpChannel amqpChannel) {
        this.amqpChannel = amqpChannel;
        this.channelId = amqpChannel.getChannelId();
        this.connection = amqpChannel.getConnection();
    }

    @Override
    public void queueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                             boolean autoDelete, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                            + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        if ((queue == null) || (queue.length() == 0)) {
            queue = AMQShortString.createAMQShortString("tmp_" + UUID.randomUUID());
        }
        AMQShortString finalQueue = queue;
        boolean createIfMissing = passive ? false : true;
        QueueContainer.setPulsarService(connection.getPulsarService());
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                QueueContainer.asyncGetQueue(connection.getNamespaceName(), finalQueue.toString(), createIfMissing);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
            } else {
                if (null == amqpQueue) {
                    amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + finalQueue);
                } else {
                    amqpChannel.checkExclusiveQueue(amqpQueue);
                    amqpChannel.setDefaultQueue(amqpQueue);

                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(finalQueue, 0, 0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            }
        });
    }

    @Override
    public void queueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                    ifUnused, ifEmpty, nowait);
        }
        if ((queue == null) || (queue.length() == 0)) {
            //get the default queue on the channel:
            AmqpQueue amqpQueue = amqpChannel.getDefaultQueue();
            delete(amqpQueue);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                    QueueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                if (throwable != null) {
                    log.error("Get Topic error:{}", throwable.getMessage());
                    amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
                } else {
                    if (null == amqpQueue) {
                        amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queue.toString());
                    } else {
                        delete(amqpQueue);
                    }
                }
            });
        }
    }

    @Override
    public void queueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                          boolean nowait, FieldTable argumentsTable) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        Map<String, Object> arguments = FieldTable.convertToMap(argumentsTable);
        if (queue == null || StringUtils.isEmpty(queue.toString())) {
            AmqpQueue amqpQueue = amqpChannel.getDefaultQueue();
            if (amqpQueue != null && bindingKey == null) {
                bindingKey = AMQShortString.valueOf(amqpQueue.getName());
            }
            bind(exchange, amqpQueue, bindingKey.toString(), arguments);
        } else {
            CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                    QueueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
            AMQShortString finalBindingKey = bindingKey;
            amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
                if (throwable != null) {
                    log.error("Get Topic error:{}", throwable.getMessage());
                    amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
                } else {
                    if (amqpQueue == null) {
                        amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                        return;
                    }
                    amqpChannel.checkExclusiveQueue(amqpQueue);
                    if (null == finalBindingKey) {
                        bind(exchange, amqpQueue, amqpQueue.getName(), arguments);
                    } else {
                        bind(exchange, amqpQueue, finalBindingKey.toString(), arguments);
                    }
                }
            });
        }
    }

    @Override
    public void queueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                            FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                    exchange, bindingKey, arguments);
        }
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                QueueContainer.asyncGetQueue(connection.getNamespaceName(), queue.toString(), false);
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
            } else {
                if (amqpQueue == null) {
                    amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                    return;
                }
                amqpChannel.checkExclusiveQueue(amqpQueue);
                String exchangeName;
                if (amqpChannel.isDefaultExchange(exchange)) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = exchange.toString();
                }
                CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                        ExchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName,
                                false, null);
                amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable1) -> {
                    if (throwable1 != null) {
                        log.error("Get Topic error:{}", throwable1.getMessage());
                        amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable1.getMessage());
                    } else {
                        try {
                            amqpQueue.unbindExchange(amqpExchange);
                            if (amqpExchange.getAutoDelete() && (amqpExchange.getQueueSize() == 0)) {
                                ExchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                                amqpExchange.getTopic().delete().get();
                            }
                            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
                            connection.writeFrame(responseBody.generateFrame(channelId));
                        } catch (Exception e) {
                            connection.sendConnectionClose(INTERNAL_ERROR, "unbind failed:" + e.getMessage(), channelId);
                        }
                    }
                });
            }
        });
    }

    @Override
    public void queuePurge(AMQShortString queue, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueuePurge[ queue: {}, nowait:{} ]", channelId, queue, nowait);
        }
        // not support in first stage.
        connection.sendConnectionClose(ErrorCodes.UNSUPPORTED_CLIENT_PROTOCOL_ERROR, "Not support yet.", channelId);
        //        MethodRegistry methodRegistry = connection.getMethodRegistry();
        //        AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(0);
        //        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    private void delete(AmqpQueue amqpQueue) {
        if (amqpQueue == null) {
            amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "Queue '" + amqpQueue.getName() + "' does not exist.");
        } else {
            amqpChannel.checkExclusiveQueue(amqpQueue);
            QueueContainer.deleteQueue(connection.getNamespaceName(), amqpQueue.getName());
//            CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
//                    ExchangeContainer.asyncGetExchange(connection.getNamespaceName(),
//                            AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE,
//                            false, AbstractAmqpExchange.Type.Direct.toString());
//            amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
//                if (throwable != null) {
//                    log.error("Get Topic error:{}", throwable.getMessage());
//                    amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
//                } else {
//                    amqpQueue.unbindExchange(amqpExchange);
//                    MethodRegistry methodRegistry = connection.getMethodRegistry();
//                    QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
//                    connection.writeFrame(responseBody.generateFrame(channelId));
//                }
//            });


            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        }
    }

    private void bind(AMQShortString exchange, AmqpQueue amqpQueue,
                      String bindingKey, Map<String, Object> arguments) {
        String exchangeName = amqpChannel.isDefaultExchange(exchange) ?
                AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE : exchange.toString();
        if (exchangeName.equals(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)) {
            amqpChannel.closeChannel(ErrorCodes.ACCESS_REFUSED, "Can not bind to default exchange ");
        }
        String exchangeType = null;
        boolean createIfMissing = false;
        if (amqpChannel.isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = amqpChannel.getExchangeType(exchange.toString());
        }

        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                ExchangeContainer.asyncGetExchange(connection.getNamespaceName(), exchangeName,
                        createIfMissing, exchangeType);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                amqpChannel.closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable.getMessage());
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
