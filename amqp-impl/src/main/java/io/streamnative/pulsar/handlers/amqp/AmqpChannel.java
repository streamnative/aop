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

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.getExchangeType;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isBuildInExchange;
import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;
import static org.apache.qpid.server.transport.util.Functions.hex;

import com.google.common.annotations.VisibleForTesting;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import io.streamnative.pulsar.handlers.amqp.flow.AmqpFlowCreditManager;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.log4j.Log4j2;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.ConsumerTagInUseException;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.TxCommitOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxRollbackOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.TxSelectOkBody;
import org.apache.qpid.server.txn.AsyncCommand;
import org.apache.qpid.server.txn.ServerTransaction;

/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    private final int channelId;
    private final AmqpConnection connection;
    private final AtomicBoolean blocking = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private final java.util.Queue<AsyncCommand> unfinishedCommandsQueue = new ConcurrentLinkedQueue<>();
    private long confirmedMessageCounter;
    private volatile ServerTransaction transaction;
    private boolean confirmOnPublish;
    /** A channel has a default queue (the last declared) that is used when no queue name is explicitly set. */
    private volatile AmqpQueue defaultQueue;

    private final UnacknowledgedMessageMap unacknowledgedMessageMap;

    /** Maps from consumer tag to consumers instance. */
    private final Map<String, Consumer> tag2ConsumersMap = new ConcurrentHashMap<>();

    private final Map<String, AmqpConsumer> fetchConsumerMap = new ConcurrentHashMap<>();

    /**
     * The current message - which may be partial in the sense that not all frames have been received yet - which has
     * been received by this channel. As the frames are received the message gets updated and once all frames have been
     * received the message can then be routed.
     */
    private IncomingMessage currentMessage;

    private final String defaultSubscription = "defaultSubscription";
    public static final AMQShortString EMPTY_STRING = AMQShortString.createAMQShortString((String) null);
    /**
     * ConsumerTag prefix, the tag is unique per subscription to a queue.
     * The server returns this in response to a basic.consume request.
     */
    private static final String CONSUMER_TAG_PREFIX = "aop.ctag-";

    /**
     * The consumer ID.
     */
    private static final AtomicLong CONSUMER_ID = new AtomicLong(0);

    /**
     * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver frame so that
     * value of this represents the <b>last</b> tag sent out.
     */
    private volatile long deliveryTag = 0;
    private final AmqpFlowCreditManager creditManager;
    private final AtomicBoolean blockedOnCredit = new AtomicBoolean(false);
    public static final int DEFAULT_CONSUMER_PERMIT = 1000;
    private ExchangeService exchangeService;
    private QueueService queueService;
    private ExchangeContainer exchangeContainer;
    private QueueContainer queueContainer;

    public AmqpChannel(int channelId, AmqpConnection connection, AmqpBrokerService amqpBrokerService) {
        this.channelId = channelId;
        this.connection = connection;
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap(this);
        this.creditManager = new AmqpFlowCreditManager(0, 0);
        this.exchangeService = amqpBrokerService.getExchangeService();
        this.queueService = amqpBrokerService.getQueueService();
        this.exchangeContainer = amqpBrokerService.getExchangeContainer();
        this.queueContainer = amqpBrokerService.getQueueContainer();
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active,
                                     boolean write, boolean read) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "RECV[{}] AccessRequest[ realm: {}, exclusive: {}, passive: {}, active: {}, write: {}, read: {} ]",
                    channelId, realm, exclusive, passive, active, write, read);
        }

        MethodRegistry methodRegistry = connection.getMethodRegistry();

        // We don't implement access control class, but to keep clients happy that expect it always use the "0" ticket.
        AccessRequestOkBody response = methodRegistry.createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(channelId));

    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable,
                                       boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDeclare[ exchange: {},"
                            + " type: {}, passive: {}, durable: {}, autoDelete: {}, internal: {}, "
                            + "nowait: {}, arguments: {} ]", channelId, exchange,
                    type, passive, durable, autoDelete, internal, nowait, arguments);
        }

        this.exchangeService.exchangeDeclare(connection.getNamespaceName(), exchange.toString(), type.toString(),
                passive, durable, autoDelete, internal, arguments).thenAccept(__ -> {
            if (!nowait) {
                connection.writeFrame(
                        connection.getMethodRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
            }
        }).exceptionally(t -> {
            log.error("Failed to declare exchange {} in vhost {}. type: {}, passive: {}, durable: {}, "
                            + "autoDelete: {}, nowait: {}", type, passive, durable, autoDelete, nowait,
                    exchange, connection.getNamespaceName(), t);
            handleAoPException(t);
            return null;
        });
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                    nowait);
        }

        exchangeService.exchangeDelete(connection.getNamespaceName(), exchange.toString(), ifUnused)
                .thenAccept(__ -> {
                    ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                    connection.writeFrame(responseBody.generateFrame(channelId));
                })
                .exceptionally(t -> {
                    log.error("Failed to delete exchange {} in vhost {}.",
                            exchange, connection.getNamespaceName(), t);
                    handleAoPException(t);
                    return null;
                });
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queueName) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                    routingKey, queueName);
        }

        exchangeService.exchangeBound(
                connection.getNamespaceName(), exchange.toString(), routingKey.toString(), queueName.toString())
                .thenAccept(replyCode -> {
                    String replyText = null;
                    switch (replyCode) {
                        case ExchangeBoundOkBody.EXCHANGE_NOT_FOUND:
                            replyText = "Exchange '" + exchange + "' not found in vhost " + connection.getNamespaceName();
                            break;
                        case ExchangeBoundOkBody.QUEUE_NOT_FOUND:
                            replyText = "Queue '" + queueName + "' not found in vhost " + connection.getNamespaceName();
                            break;
                        default:
                            // do nothing
                    }

                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                            .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(replyText));
                    connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
                })
                .exceptionally(t -> {
                    log.error("Failed to bound queue {} to exchange {} with routingKey {} in vhost {}",
                            queueName, exchange, routingKey, connection.getNamespaceName(), t);
                    handleAoPException(t);
                    return null;
                });
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                                    boolean autoDelete, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                            + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        queueService.queueDeclare(connection.getNamespaceName(), queue.toString(), passive, durable, exclusive,
                autoDelete, nowait, arguments, connection.getConnectionId()).thenAccept(amqpQueue -> {
            setDefaultQueue(amqpQueue);
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(
                    AMQShortString.createAMQShortString(amqpQueue.getName()), 0, 0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        }).exceptionally(t -> {
            log.error("Failed to declare queue {} in vhost {}", queue, connection.getNamespaceName(), t);
            handleAoPException(t);
            return null;
        });
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable argumentsTable) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                    channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        queueService.queueBind(connection.getNamespaceName(), getQueueName(queue),
                exchange.toString(), bindingKey.toString(), nowait, argumentsTable,
                connection.getConnectionId()).thenAccept(__ -> {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
        }).exceptionally(t -> {
            log.error("Failed to bind queue {} to exchange {}.", queue, exchange, t);
            handleAoPException(t);
            return null;
        });
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueuePurge[ queue: {}, nowait:{} ]", channelId, queue, nowait);
        }
        queueService.queuePurge(connection.getNamespaceName(), queue.toString(), nowait, connection.getConnectionId())
                .thenAccept(__ -> {
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }).exceptionally(t -> {
                    log.error("Failed to purge queue {} in vhost {}", queue, connection.getNamespaceName(), t);
                    handleAoPException(t);
                    return null;
                });
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                    ifUnused, ifEmpty, nowait);
        }
//        String queueName;
//        if (queue == null || queue.length() == 0) {
//            if (getDefaultQueue() != null) {
//                checkExclusiveQueue(getDefaultQueue());
//                queueName = getDefaultQueue().getName();
//            } else {
//                closeChannel(ErrorCodes.ARGUMENT_INVALID, "The queue name is empty.");
//                return;
//            }
//        } else {
//            queueName = queue.toString();
//        }
        queueService.queueDelete(connection.getNamespaceName(), getQueueName(queue), ifUnused,
                        ifEmpty, connection.getConnectionId())
                .thenAccept(__ -> {
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(0);
                    connection.writeFrame(responseBody.generateFrame(channelId));
                })
                .exceptionally(t -> {
                    log.error("Failed to delete queue " + queue, t);
                    handleAoPException(t);
                    return null;
                });
    }

    private String getQueueName(AMQShortString queue) {
        String queueName;
        if (queue == null || queue.length() == 0) {
            if (getDefaultQueue() != null) {
                queueName = getDefaultQueue().getName();
            } else {
                return null;
            }
        } else {
            queueName = queue.toString();
        }
        return queueName;
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                   FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                    exchange, bindingKey, arguments);
        }
        queueService.queueUnbind(connection.getNamespaceName(), queue.toString(), exchange.toString(),
                bindingKey.toString(), arguments, connection.getConnectionId()).thenAccept(__ -> {
            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
        }).exceptionally(t -> {
            log.error("Failed to unbind queue {} with exchange {} in vhost {}",
                    queue, exchange, connection.getNamespaceName(), t);
            handleAoPException(t);
            return null;
        });
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicQos[prefetchSize: {} prefetchCount: {} global: {}]",
                channelId, prefetchSize, prefetchCount, global);
        }
        if (prefetchSize > 0) {
            closeChannel(ErrorCodes.NOT_IMPLEMENTED, "prefetchSize not supported ");
        }
        creditManager.setCreditLimits(0, prefetchCount);
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createBasicQosOkBody();
        connection.writeFrame(responseBody.generateFrame(getChannelId()));
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag,
                                    boolean noLocal, boolean noAck, boolean exclusive,
                                    boolean nowait, FieldTable arguments) {

        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicConsume[queue:{} consumerTag:{} noLocal:{} noAck:{} exclusive:{} nowait:{}"
                    + " arguments:{}]", channelId, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
        }
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueService.getQueue(connection.getNamespaceName(), queue.toString(), false,
                        connection.getConnectionId());
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get the queue from the queue container", throwable);
                handleAoPException(throwable);
            } else {
                if (amqpQueue == null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
                } else {
                    PersistentTopic indexTopic = ((PersistentQueue) amqpQueue).getIndexTopic();
                    subscribe(getConsumerTag(consumerTag), queue.toString(), indexTopic, noAck, exclusive, nowait);
                }
            }
        });
    }

    private String getConsumerTag(AMQShortString consumerTag) {
        if (consumerTag == null) {
            return CONSUMER_TAG_PREFIX + connection.remoteAddress + "-" + UUID.randomUUID();
        } else {
            // don't change the consumer tag if the consumerTag is existing
            return consumerTag.toString();
        }
    }

    private synchronized void subscribe(String consumerTag, String queueName, Topic topic,
                           boolean ack, boolean exclusive, boolean nowait){

        CompletableFuture<Void> exceptionFuture = new CompletableFuture<>();
        exceptionFuture.whenComplete((ignored, e) -> {
            if (e != null) {
                closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
                log.error("BasicConsume error queue:{} consumerTag:{} ex {}",
                        queueName, consumerTag, e.getMessage());
            }
        });

        if (tag2ConsumersMap.containsKey(consumerTag)) {
            exceptionFuture.completeExceptionally(
                    new ConsumerTagInUseException("Consumer already exists with same consumerTag: " + consumerTag));
            return;
        }

        CompletableFuture<Subscription> subscriptionFuture = topic.createSubscription(
                defaultSubscription, CommandSubscribe.InitialPosition.Earliest, false, null);
        subscriptionFuture.thenAccept(subscription -> {
            AmqpConsumer consumer;
            try {
                consumer = new AmqpConsumer(queueContainer, subscription, exclusive
                        ? CommandSubscribe.SubType.Exclusive :
                        CommandSubscribe.SubType.Shared, topic.getName(), CONSUMER_ID.incrementAndGet(), 0,
                        consumerTag, true, connection.getServerCnx(), "", null,
                        false, CommandSubscribe.InitialPosition.Latest,
                        null, this, consumerTag, queueName, ack);
            } catch (BrokerServiceException e) {
                exceptionFuture.completeExceptionally(e);
                return;
            }
            subscription.addConsumer(consumer);
            consumer.handleFlow(DEFAULT_CONSUMER_PERMIT);
            tag2ConsumersMap.put(consumerTag, consumer);

            if (!nowait) {
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.
                        createBasicConsumeOkBody(AMQShortString.
                                createAMQShortString(consumer.getConsumerTag()));
                connection.writeFrame(responseBody.generateFrame(channelId));
            }
            exceptionFuture.complete(null);
        });
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicCancel[ consumerTag: {}  noWait: {} ]", channelId, consumerTag, noWait);
        }

        unsubscribeConsumer(AMQShortString.toString(consumerTag));
        if (!noWait) {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            BasicCancelOkBody cancelOkBody = methodRegistry.createBasicCancelOkBody(consumerTag);
            connection.writeFrame(cancelOkBody.generateFrame(channelId));
        }
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
                                    boolean immediate) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicPublish[exchange: {} routingKey: {} mandatory: {} immediate: {}]",
                    channelId, exchange, routingKey, mandatory, immediate);
        }
        if (isDefaultExchange(exchange)) {
            CompletableFuture<AmqpExchange> completableFuture = exchangeContainer.
                    asyncGetExchange(connection.getNamespaceName(), AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE,
                            true, ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
            completableFuture.whenComplete((amqpExchange, throwable) -> {
                if (null != throwable) {
                    log.error("Get exchange failed. exchange name:{}", AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
                    closeChannel(ErrorCodes.INTERNAL_ERROR, "Get exchange failed. ");
                } else {
                    String queueName = routingKey.toString();
                    CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                            queueContainer.asyncGetQueue(connection.getNamespaceName(), queueName, false);
                    amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable1) -> {
                        if (throwable1 != null) {
                            log.error("Get Topic error:{}", throwable1.getMessage());
                            closeChannel(INTERNAL_ERROR, "Get Topic error: " + throwable1.getMessage());
                        } else {
                            if (amqpQueue == null) {
                                closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queueName);
                                return;
                            } else {
                                // bind to default exchange.
                                if (amqpQueue.getRouter(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE) == null) {
                                    amqpQueue.bindExchange(amqpExchange,
                                            AbstractAmqpMessageRouter.generateRouter(AmqpExchange.Type.Direct),
                                            routingKey.toString(), null);
                                }
                                MessagePublishInfo info = new MessagePublishInfo(AMQShortString.
                                        valueOf(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE), immediate,
                                        mandatory, routingKey);
                                setPublishFrame(info, null);
                            }
                        }
                    }).join();
                }
            }).join();
        } else {
            MessagePublishInfo info = new MessagePublishInfo(exchange, immediate,
                    mandatory, routingKey);
            setPublishFrame(info, null);
        }
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        String queueName = AMQShortString.toString(queue);
        CompletableFuture<AmqpQueue> amqpQueueCompletableFuture =
                queueService.getQueue(connection.getNamespaceName(), queue.toString(), false,
                        connection.getConnectionId());
        amqpQueueCompletableFuture.whenComplete((amqpQueue, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                handleAoPException(throwable);
            } else {
                if (amqpQueue == null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue: " + queueName);
                    return;
                } else {
                    Topic topic = amqpQueue.getTopic();
                    AmqpConsumer amqpConsumer = fetchConsumerMap.computeIfAbsent(queueName, value -> {

                        Subscription subscription = topic.getSubscription(defaultSubscription);
                        AmqpConsumer consumer;
                        try {
                            if (subscription == null) {
                                subscription = topic.createSubscription(defaultSubscription,
                                        CommandSubscribe.InitialPosition.Earliest, false, null).get();
                            }
                            consumer = new AmqpPullConsumer(queueContainer, subscription,
                                    CommandSubscribe.SubType.Shared,
                                    topic.getName(), 0, 0, "", true,
                                    connection.getServerCnx(), "", null, false,
                                    CommandSubscribe.InitialPosition.Latest, null, this,
                                    "", queueName, noAck);
                            subscription.addConsumer(consumer);
                            consumer.handleFlow(DEFAULT_CONSUMER_PERMIT);
                            return consumer;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return null;
                    });
                    MessageFetchContext.handleFetch(this, amqpConsumer, noAck);
                }
            }
        });
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ChannelFlow[active: {}]", channelId, active);
        }
        // TODO channelFlow process
        ChannelFlowOkBody body = connection.getMethodRegistry().createChannelFlowOkBody(true);
        connection.writeFrame(body.generateFrame(channelId));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ChannelFlowOk[active: {}]", channelId, active);
        }
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ChannelClose[replyCode: {} replyText: {} classId: {} methodId: {}",
                    channelId, replyCode, replyText, classId, methodId);
        }
        // TODO Process outstanding client requests
        processAsync();
        connection.closeChannel(this);
        connection.writeFrame(new AMQFrame(getChannelId(), connection.getMethodRegistry().createChannelCloseOkBody()));
    }

    @Override
    public void receiveChannelCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] ChannelCloseOk", channelId);
        }

        connection.closeChannelOk(getChannelId());
    }

    private boolean hasCurrentMessage() {
        return currentMessage != null;
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        if (log.isDebugEnabled()) {
            int binaryDataLimit = 2000;
            log.debug("RECV[{}] MessageContent[data:{}]", channelId, hex(data, binaryDataLimit));
        }

        if (hasCurrentMessage()) {
            publishContentBody(new ContentBody(data));
        } else {
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentBody(ContentBody contentBody) {
        if (log.isDebugEnabled()) {
            log.debug("content body received on channel {}", channelId);
        }

        try {
            long currentSize = currentMessage.addContentBodyFrame(contentBody);
            if (currentSize > currentMessage.getSize()) {
                connection.sendConnectionClose(ErrorCodes.FRAME_ERROR,
                        "More message data received than content header defined", channelId);
            } else {
                deliverCurrentMessageIfComplete();
            }
        } catch (RuntimeException e) {
            // we want to make sure we don't keep a reference to the message in the
            // event of an error
            currentMessage = null;
            throw e;
        }
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] MessageHeader[ properties: {{}} bodySize: {}]", channelId, properties, bodySize);
        }

        // TODO - maxMessageSize ?
        long maxMessageSize = 1024 * 1024 * 10;
        if (hasCurrentMessage()) {
            if (bodySize > maxMessageSize) {
                properties.dispose();
                closeChannel(ErrorCodes.MESSAGE_TOO_LARGE,
                        "Message size of " + bodySize + " greater than allowed maximum of " + maxMessageSize);
            } else {
                publishContentHeader(new ContentHeaderBody(properties, bodySize));
            }
        } else {
            properties.dispose();
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentHeader(ContentHeaderBody contentHeaderBody) {
        if (log.isDebugEnabled()) {
            log.debug("Content header received on channel {}", channelId);
        }

        currentMessage.setContentHeaderBody(contentHeaderBody);

        deliverCurrentMessageIfComplete();
    }

    private void deliverCurrentMessageIfComplete() {
        if (currentMessage.allContentReceived()) {
            MessagePublishInfo info = currentMessage.getMessagePublishInfo();
            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                connection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.", channelId);
                return;
            }
            boolean createIfMissing = false;
            String exchangeType = null;
            if (isDefaultExchange(AMQShortString.valueOf(exchangeName))
                    || isBuildInExchange(exchangeName)) {
                // Auto create default and buildIn exchanges if use.
                createIfMissing = true;
                exchangeType = getExchangeType(exchangeName);
            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
            }
            CompletableFuture<AmqpExchange> completableFuture = exchangeContainer.
                    asyncGetExchange(connection.getNamespaceName(), exchangeName, createIfMissing, exchangeType);
            completableFuture.thenApply(amqpExchange -> amqpExchange.writeMessageAsync(message, routingKey).
                    thenApply(position -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Publish message success, position {}", position.toString());
                        }
                        if (confirmOnPublish) {
                            confirmedMessageCounter++;
                            BasicAckBody body = connection.getMethodRegistry().
                                    createBasicAckBody(confirmedMessageCounter, false);
                            connection.writeFrame(body.generateFrame(channelId));
                        }
                        return position;
                    })).exceptionally(throwable -> {
                log.error("Failed to write message to exchange", throwable);
                return null;
            });
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] BasicNAck[deliveryTag: {} multiple: {} requeue: {}]",
                channelId, deliveryTag, multiple, requeue);
        }
        messageNAck(deliveryTag, multiple, requeue);
    }

    public void messageNAck(long deliveryTag, boolean multiple, boolean requeue) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            if (requeue) {
                requeue(ackedMessages);
            }
        } else {
            closeChannel(ErrorCodes.IN_USE, "deliveryTag not found");
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledgeAll();
        if (!ackedMessages.isEmpty()) {
            requeue(ackedMessages);
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }
        if (sync) {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            AMQMethodBody recoverOk = methodRegistry.createBasicRecoverSyncOkBody();
            connection.writeFrame(recoverOk.generateFrame(getChannelId()));
        }
    }

    private void requeue(Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> messages) {
        Map<AmqpConsumer, List<PositionImpl>> positionMap = new HashMap<>();
        messages.stream().forEach(association -> {
            AmqpConsumer consumer = association.getConsumer();
            List<PositionImpl> positions = positionMap.computeIfAbsent(consumer,
                list -> new ArrayList<>());
            positions.add((PositionImpl) association.getPosition());
        });
        positionMap.entrySet().stream().forEach(entry -> {
            entry.getKey().redeliverAmqpMessages(entry.getValue());
        });
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] BasicAck[deliveryTag: {} multiple: {} ]", channelId, deliveryTag, multiple);
        }
        messageAck(deliveryTag, multiple);
    }

    private void messageAck(long deliveryTag, boolean multiple) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
            unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            ackedMessages.stream().forEach(entry -> {
                entry.getConsumer().messagesAck(entry.getPosition());
            });
        } else {
            closeChannel(ErrorCodes.IN_USE, "deliveryTag not found");
        }
        if (creditManager.hasCredit() && isBlockedOnCredit()) {
            unBlockedOnCredit();
        }

    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        messageNAck(deliveryTag, false, requeue);
    }

    @Override
    public void receiveTxSelect() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxSelect", channelId);
        }
        // TODO txSelect process
        TxSelectOkBody txSelectOkBody = connection.getMethodRegistry().createTxSelectOkBody();
        connection.writeFrame(txSelectOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveTxCommit() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxCommit", channelId);
        }
        // TODO txCommit process
        TxCommitOkBody txCommitOkBody = connection.getMethodRegistry().createTxCommitOkBody();
        connection.writeFrame(txCommitOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveTxRollback() {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] TxRollback", channelId);
        }
        // TODO txRollback process
        TxRollbackOkBody txRollbackBody = connection.getMethodRegistry().createTxRollbackOkBody();
        connection.writeFrame(txRollbackBody.generateFrame(channelId));
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ConfirmSelect [ nowait: {} ]", channelId, nowait);
        }
        confirmOnPublish = true;

        if (!nowait) {
            connection.writeFrame(new AMQFrame(channelId, ConfirmSelectOkBody.INSTANCE));
        }
    }

    public void receivedComplete() {
        processAsync();
    }

    private void sendChannelClose(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public void processAsync() {
        // TODO
    }

    public void close() {
        // TODO
        unsubscribeConsumerAll();
        // TODO need to delete exclusive queues in this channel.
        setDefaultQueue(null);
    }

    public synchronized void block() {
        // TODO
    }

    public synchronized void unblock() {
        // TODO
    }

    public int getChannelId() {
        return channelId;
    }

    public boolean isClosing() {
        return closing.get() || connection.isClosing();
    }

    public boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    public void closeChannel(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public long getNextDeliveryTag() {
        return ++deliveryTag;
    }

    public AmqpConnection getConnection() {
        return connection;
    }

    public UnacknowledgedMessageMap getUnacknowledgedMessageMap() {
        return unacknowledgedMessageMap;
    }

    private boolean unsubscribeConsumer(String consumerTag) {
        if (log.isDebugEnabled()) {
            log.debug("Unsubscribing consumer '{}' on channel {}", consumerTag, this);
        }

        Consumer consumer = tag2ConsumersMap.remove(consumerTag);
        if (consumer != null) {
            try {
                consumer.close();
                return true;
            } catch (BrokerServiceException e) {
                log.error(e.getMessage());
            }

        } else {
            log.warn("Attempt to unsubscribe consumer with tag  {} which is not registered.", consumerTag);
        }
        return false;
    }

    private void unsubscribeConsumerAll() {
        if (log.isDebugEnabled()) {
            if (!tag2ConsumersMap.isEmpty()) {
                log.debug("Unsubscribing all consumers on channel  {}", channelId);
            } else {
                log.debug("No consumers to unsubscribe on channel {}", channelId);
            }
        }
        try {
            tag2ConsumersMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }

            });
            tag2ConsumersMap.clear();
            fetchConsumerMap.forEach((key, value) -> {
                try {
                    value.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }

            });
            fetchConsumerMap.clear();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    protected void setDefaultQueue(AmqpQueue queue) {
        defaultQueue = queue;
    }

    protected AmqpQueue getDefaultQueue() {
        return defaultQueue;
    }

//    public void checkExclusiveQueue(AmqpQueue amqpQueue) {
//        if (amqpQueue != null && amqpQueue.isExclusive()
//            && (amqpQueue.getConnectionId() != connection.getConnectionId())) {
//            closeChannel(ErrorCodes.ALREADY_EXISTS,
//                "Exclusive queue can not be used form other connection, queueName: '" + amqpQueue.getName() + "'");
//        }
//    }

    @VisibleForTesting
    public Map<String, Consumer> getTag2ConsumersMap() {
        return tag2ConsumersMap;
    }

    public void restoreCredit(final int count, final long size) {
        creditManager.restoreCredit(count, size);
    }

    public boolean setBlockedOnCredit() {
        return this.blockedOnCredit.compareAndSet(false, true);
    }

    public boolean isBlockedOnCredit() {
        if (log.isDebugEnabled()) {
            log.debug("isBlockedOnCredit {}", blockedOnCredit.get());
        }
        return this.blockedOnCredit.get();
    }

    public void unBlockedOnCredit() {
        if (this.blockedOnCredit.compareAndSet(true, false)) {
            notifyAllConsumers();
        }
    }

    private void notifyAllConsumers() {
        tag2ConsumersMap.values().stream().forEach(consumer -> {
            ((AmqpConsumer) consumer).handleFlow(1);
        });
    }

    public AmqpFlowCreditManager getCreditManager() {
        return creditManager;
    }

    private void handleAoPException(Throwable t) {
        if (!(t instanceof AoPException)) {
            connection.sendConnectionClose(INTERNAL_ERROR, "Internal error: " + t.getMessage(), channelId);
            return;
        }
        AoPException exception = (AoPException) t;
        if (exception.isCloseChannel()) {
            closeChannel(exception.getErrorCode(), exception.getMessage());
        }
        if (exception.isCloseConnection()) {
            connection.sendConnectionClose(exception.getErrorCode(), exception.getMessage(), channelId);
        }
    }

}
