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
import static org.apache.qpid.server.protocol.ErrorCodes.NOT_FOUND;
import static org.apache.qpid.server.transport.util.Functions.hex;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.streamnative.pulsar.handlers.amqp.flow.AmqpFlowCreditManager;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryExchange;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryQueue;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.log4j.Log4j2;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
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
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
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

    private AmqpTopicManager amqpTopicManager;
    private final String defaultSubscription = "defaultSubscription";
    public static final AMQShortString EMPTY_STRING = AMQShortString.createAMQShortString((String) null);
    /**
     * This tag is unique per subscription to a queue. The server returns this in response to a basic.consume request.
     */
    private volatile int consumerTag;

    /**
     * The delivery tag is unique per channel. This is pre-incremented before putting into the deliver frame so that
     * value of this represents the <b>last</b> tag sent out.
     */
    private volatile long deliveryTag = 0;
    private AmqpFlowCreditManager creditManager;

    public AmqpChannel(int channelId, AmqpConnection connection) {
        this.channelId = channelId;
        this.connection = connection;
        this.amqpTopicManager = connection.getAmqpTopicManager();
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap(this);
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

        String exchangeName = exchange.toString().
                replaceAll("\r", "").
                replaceAll("\n", "").trim();
        final MethodRegistry methodRegistry = connection.getMethodRegistry();
        final AMQMethodBody declareOkBody = methodRegistry.createExchangeDeclareOkBody();

        if (isDefaultExchange(exchange)) {
            if (!AMQShortString.createAMQShortString(ExchangeDefaults.DIRECT_EXCHANGE_CLASS).equals(type)) {
                StringBuffer sb = new StringBuffer();
                sb.append("Attempt to redeclare default exchange: of type")
                        .append(ExchangeDefaults.DIRECT_EXCHANGE_CLASS).append("to").append(type).append(".");
                connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, sb.toString(), channelId);
            } else {
                // in-memory integration
                if (!durable) {
                    InMemoryExchange inMemoryExchange = new InMemoryExchange(
                            exchangeName, AmqpExchange.Type.value(type.toString()), autoDelete);
                    ExchangeContainer.putExchange(connection.getNamespaceName(), exchange.toString(), inMemoryExchange);
                }
                if (!nowait) {
                    sync();
                    // if declare a default exchange, return success.
                    connection.writeFrame(declareOkBody.generateFrame(channelId));
                }
            }
        } else {
            AmqpExchange amqpExchange = ExchangeContainer.getExchange(connection.getNamespaceName(), exchangeName);
            if (passive) {
                if (null == amqpExchange) {
                    if (durable) {
                        TopicName topicName = TopicName.get(
                            TopicDomain.persistent.value(), connection.getNamespaceName(), exchangeName);
                        CompletableFuture<Topic> tf = amqpTopicManager.getTopic(topicName.toString(), false);
                        tf.whenComplete((t, e) -> {
                            if (e != null) {
                                closeChannel(INTERNAL_ERROR, e.getMessage());
                                return;
                            }
                            if (t == null) {
                                closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + exchangeName + "'");
                                return;
                            }
                            ExchangeContainer.
                                putExchange(connection.getNamespaceName(), exchangeName,
                                    new PersistentExchange(exchangeName, AmqpExchange.Type.value(
                                        type.toString()), (PersistentTopic) t, amqpTopicManager, autoDelete));
                            if (!nowait) {
                                sync();
                                connection.writeFrame(declareOkBody.generateFrame(channelId));
                            }
                        });
                    } else {
                        closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + exchangeName + "'");
                    }
                } else if (!(type == null || type.length() == 0)
                        && !amqpExchange.getType().toString().equalsIgnoreCase(type.toString())) {
                    connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Attempt to redeclare exchange: '"
                            + exchangeName
                            + "' of type "
                            + amqpExchange.getType()
                            + " to "
                            + type
                            + ".", getChannelId());
                } else if (!nowait) {
                    sync();
                    connection.writeFrame(declareOkBody.generateFrame(getChannelId()));
                }
            } else {
                if (null != amqpExchange) {
                    if (!amqpExchange.getType().toString().equalsIgnoreCase(type.toString())
                            || !amqpExchange.getAutoDelete() == autoDelete
                            || !amqpExchange.getDurable() == durable) {
                        connection.sendConnectionClose(ErrorCodes.IN_USE, "Attempt to redeclare exchange: '"
                                + exchangeName
                                + "' of type "
                                + amqpExchange.getType()
                                + " to "
                                + type
                                + ".", getChannelId());
                    } else {
                        if (!nowait) {
                            sync();
                            connection.writeFrame(declareOkBody.generateFrame(channelId));
                        }
                    }
                } else {
                    // create new exchange, on first step, we just create a Pulsar Topic.
                    if (PulsarService.State.Started == connection.getPulsarService().getState()) {

                        if (!durable) {
                            // in-memory integration
                            InMemoryExchange inMemoryExchange = new InMemoryExchange(
                                    exchangeName, AmqpExchange.Type.value(type.toString()), autoDelete);
                            ExchangeContainer.putExchange(connection.getNamespaceName(),
                                    exchangeName, inMemoryExchange);
                            if (!nowait) {
                                sync();
                                connection.writeFrame(declareOkBody.generateFrame(channelId));
                            }
                            return;
                        }

                        TopicName topicName = TopicName.get(
                                TopicDomain.persistent.value(), connection.getNamespaceName(), exchangeName);
                        try {
                            PersistentTopic persistentTopic = (PersistentTopic) amqpTopicManager.getOrCreateTopic(
                                    topicName.toString(), true);
                            if (persistentTopic == null) {
                                connection.sendConnectionClose(INTERNAL_ERROR,
                                        "AOP Create Exchange failed.", channelId);
                                return;
                            }
                            ExchangeContainer.
                                    putExchange(connection.getNamespaceName(), exchangeName,
                                            new PersistentExchange(exchangeName, AmqpExchange.Type.value(
                                                    type.toString()), persistentTopic, amqpTopicManager, autoDelete));
                            if (!nowait) {
                                sync();
                                connection.writeFrame(declareOkBody.generateFrame(channelId));
                            }
                        } catch (Exception e) {
                            log.error(channelId + "Exchange declare failed! exchangeName: " + exchangeName, e);
                            connection.sendConnectionClose(INTERNAL_ERROR, "AOP Create Exchange failed.", channelId);
                        }
                    } else {
                        connection.sendConnectionClose(INTERNAL_ERROR, "PulsarService not start.", channelId);
                    }
                }
            }
        }
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                    nowait);
        }
        String exchangeName = exchange.toString().
                replaceAll("\r", "").
                replaceAll("\n", "").trim();
        if (isDefaultExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Default Exchange cannot be deleted. ", channelId);
        } else {
            AmqpExchange amqpExchange = ExchangeContainer.getExchange(connection.getNamespaceName(), exchangeName);
            if (null == amqpExchange) {
                closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchange + "'");
            } else {
                if (amqpExchange.getDurable()) {
                    PersistentTopic topic = (PersistentTopic) amqpExchange.getTopic();
                    if (ifUnused && topic.getSubscriptions().isEmpty()) {
                        closeChannel(ErrorCodes.IN_USE, "Exchange has bindings. ");
                    } else {
                        try {
                            amqpTopicManager.deleteTopic(exchangeName);
                            ExchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                            topic.delete().get();
                            ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().
                                    createExchangeDeleteOkBody();
                            connection.writeFrame(responseBody.generateFrame(channelId));
                        } catch (Exception e) {
                            connection.sendConnectionClose(INTERNAL_ERROR,
                                    "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
                        }
                    }
                } else {
                    // Is this exchange in used?
                    if (amqpExchange.getQueueSize() > 0) {
                        closeChannel(ErrorCodes.IN_USE, "Exchange has bindings. ");
                    }
                    amqpTopicManager.deleteTopic(exchangeName);
                    ExchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                    ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            }
        }
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queueName) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                    routingKey, queueName);
        }
        int replyCode;
        StringBuilder replyText = new StringBuilder();
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                connection.getNamespaceName(), exchange.toString());

        Topic topic = amqpTopicManager.getOrCreateTopic(topicName.toString(), false);
        if (null == topic) {
            replyCode = ExchangeBoundOkBody.EXCHANGE_NOT_FOUND;
            replyText = replyText.insert(0, "Exchange '").append(exchange).append("' not found");
        } else {
            List<String> subs = topic.getSubscriptions().keys();
            if (null == subs || subs.isEmpty()) {
                replyCode = ExchangeBoundOkBody.QUEUE_NOT_FOUND;
                replyText = replyText.insert(0, "Queue '").append(queueName).append("' not found");
            } else {
                replyCode = ExchangeBoundOkBody.OK;
                replyText = null;
            }
        }
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(replyText));
        connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
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
        AmqpQueue amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), queue.toString());
        if (passive) {
            if (null == amqpQueue) {
                closeChannel(ErrorCodes.NOT_FOUND, "No such queue: '" + queue.toString() + "'");
            } else {
                checkExclusiveQueue(amqpQueue);
                setDefaultQueue(amqpQueue);
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
                connection.writeFrame(responseBody.generateFrame(channelId));
            }
        } else {
            checkExclusiveQueue(amqpQueue);
            if (amqpQueue != null) {
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
                connection.writeFrame(responseBody.generateFrame(channelId));
                return;
            }
            if (!durable) {
                // in-memory integration
                amqpQueue = new InMemoryQueue(queue.toString(), connection.getConnectionId(), exclusive, autoDelete);
            } else {
                try {
                    String indexTopicName = PersistentQueue.getIndexTopicName(
                            connection.getNamespaceName(), queue.toString());
                    PersistentTopic indexTopic = (PersistentTopic) amqpTopicManager
                            .getOrCreateTopic(indexTopicName, true);
                    amqpQueue = new PersistentQueue(queue.toString(), indexTopic, connection.getConnectionId(),
                            exclusive, autoDelete);
                } catch (Exception e) {
                    log.error(channelId + "Queue declare failed! queueName: {}", queue.toString());
                    connection.sendConnectionClose(INTERNAL_ERROR, "AOP Create Queue failed.", channelId);
                    return;
                }
            }
            QueueContainer.putQueue(connection.getNamespaceName(), queue.toString(), amqpQueue);
            setDefaultQueue(amqpQueue);
            // return success.
            // when call QueueBind, then create Pulsar sub.
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
            connection.writeFrame(responseBody.generateFrame(channelId));
        }
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable argumentsTable) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",

                    channelId, queue, exchange, bindingKey, nowait, argumentsTable);
        }
        Map<String, Object> arguments = FieldTable.convertToMap(argumentsTable);
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                connection.getNamespaceName(), exchange.toString());

        AmqpQueue amqpQueue;
        if (queue == null) {
            amqpQueue = getDefaultQueue();
            if (amqpQueue != null) {
                if (bindingKey == null) {
                    bindingKey = AMQShortString.valueOf(amqpQueue.getName());
                }
            }
        } else {
            amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), queue.toString());
        }
        checkExclusiveQueue(amqpQueue);
        AmqpExchange amqpExchange = ExchangeContainer.
                getExchange(connection.getNamespaceName(), exchange.toString());

        AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.generateRouter(amqpExchange.getType());
        if (messageRouter == null) {
            connection.sendConnectionClose(INTERNAL_ERROR, "Unsupported router type!", channelId);
            return;
        }

        // in-memory integration
        if (amqpQueue instanceof InMemoryQueue && amqpExchange instanceof InMemoryExchange) {
            amqpQueue.bindExchange(amqpExchange, messageRouter, AMQShortString.toString(bindingKey), arguments);
            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueBindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
            return;
        }

        Topic topic = amqpTopicManager.getOrCreateTopic(topicName.toString(), false);
        if (null == topic) {
            closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchange + "'");
        } else {
            try {
                amqpQueue.bindExchange(amqpExchange, messageRouter, AMQShortString.toString(bindingKey), arguments);
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            } catch (Exception e) {
                connection.sendConnectionClose(INTERNAL_ERROR,
                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
            }
        }
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueuePurge[ queue: {}, nowait:{} ]", channelId, queue, nowait);
        }
        // not support in first stage.
        connection.sendConnectionClose(ErrorCodes.UNSUPPORTED_CLIENT_PROTOCOL_ERROR, "Not support yet.", channelId);
        //        MethodRegistry methodRegistry = connection.getMethodRegistry();
        //        AMQMethodBody responseBody = methodRegistry.createQueuePurgeOkBody(0);
        //        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                    ifUnused, ifEmpty, nowait);
        }
        // TODO
        // return success.
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeleteOkBody responseBody = methodRegistry.createQueueDeleteOkBody(1);
        connection.writeFrame(responseBody.generateFrame(channelId));

    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                   FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId, queue,
                    exchange, bindingKey, arguments);
        }

        // in-memory integration
        AmqpQueue amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), queue.toString());
        checkExclusiveQueue(amqpQueue);
        AmqpExchange amqpExchange = ExchangeContainer.
                getExchange(connection.getNamespaceName(), exchange.toString());
        if (amqpQueue instanceof InMemoryQueue && amqpExchange instanceof InMemoryExchange) {
            amqpQueue.unbindExchange(amqpExchange);
            AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
        }

        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                connection.getNamespaceName(), exchange.toString());

        Topic topic = amqpTopicManager.getOrCreateTopic(topicName.toString(), false);
        if (null == topic) {
            connection.sendConnectionClose(INTERNAL_ERROR, "exchange not found.", channelId);
        } else {
            try {
                amqpQueue.unbindExchange(amqpExchange);
                final AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            } catch (Exception e) {
                connection.sendConnectionClose(INTERNAL_ERROR, "unbind failed:" + e.getMessage(), channelId);
            }
        }
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {

    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicQos[prefetchSize: {} prefetchCount: {} global: {}]",
                channelId, prefetchSize, prefetchCount, global);
        }

        setCredit(prefetchSize, prefetchCount);

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
                    + "arguments:{}]", channelId, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
        }
        final String consumerTag1;
        if (consumerTag == null) {
            consumerTag1 = "consumerTag_" + getNextConsumerTag();
        } else {
            consumerTag1 = consumerTag.toString();
        }

        AmqpQueue amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), queue.toString());
        if (amqpQueue == null) {
            closeChannel(ErrorCodes.NOT_FOUND, "No such queue, '" + queue.toString() + "'");
            return;
        }
        checkExclusiveQueue(amqpQueue);
        // in-memory integration
        if (amqpQueue instanceof InMemoryQueue) {
            try {
                if (!nowait) {
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.
                        createBasicConsumeOkBody(AMQShortString.createAMQShortString(consumerTag1));
                    connection.writeFrame(responseBody.generateFrame(channelId));
                    amqpQueue.readEntryAsync("ex1", 1, 1)
                        .whenComplete((entry, throwable) -> {
                            if (entry != null) {
                                try {
                                    connection.getAmqpOutputConverter().writeDeliver(
                                        MessageConvertUtils.entryToAmqpBody(entry),
                                        channelId,
                                        false,
                                        getNextDeliveryTag(),
                                        AMQShortString.createAMQShortString(consumerTag1));
                                } catch (Exception e) {
                                    closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
                                }
                            }
                        });
                    return;
                }
            } catch (Exception e) {
                closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
            }
        } else if (amqpQueue instanceof PersistentQueue) {
            PersistentTopic indexTopic = ((PersistentQueue) amqpQueue).getIndexTopic();
            if (indexTopic == null) {
                closeChannel(ErrorCodes.SYNTAX_ERROR, "system error");
                log.error("BasicConsume error queue`s indexTopic is null queue:{} consumerTag:{} ",
                    queue, consumerTag);
                return;
            }

            try {
                AmqpConsumer consumer = subscribe(consumerTag1, queue.toString(), indexTopic,
                    noAck, exclusive);
                if (!nowait) {
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.
                        createBasicConsumeOkBody(AMQShortString.
                            createAMQShortString(consumer.getConsumerTag()));
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }
            } catch (Exception e) {
                closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
                log.error("BasicConsume error queue:{} consumerTag:{} ex {}",
                    queue, consumerTag, e.getMessage());
            }

        } else {
            closeChannel(ErrorCodes.SYNTAX_ERROR, "system error");
            log.error("BasicConsume error queue type not find queue:{} consumerTag:{} ",
                queue, consumerTag);
        }
    }

    private AmqpConsumer subscribe(String consumerTag, String queueName, Topic topic,
        boolean ack, boolean exclusive) throws ConsumerTagInUseException,
        InterruptedException, ExecutionException, BrokerServiceException {
        if (consumerTag == null) {
            consumerTag = "consumerTag_" + getNextConsumerTag();
        }

        if (tag2ConsumersMap.containsKey(consumerTag)) {
            throw new ConsumerTagInUseException("Consumer already exists with same consumerTag: " + consumerTag);
        }
        Subscription subscription = topic.getSubscription(defaultSubscription);
        AmqpConsumer consumer;
        try {
            if (subscription == null) {
                subscription = topic.createSubscription(defaultSubscription,
                    PulsarApi.CommandSubscribe.InitialPosition.Earliest, false).get();
            }
            consumer = new AmqpConsumer(subscription, exclusive ? PulsarApi.CommandSubscribe.SubType.Exclusive :
                PulsarApi.CommandSubscribe.SubType.Shared, topic.getName(), 0, 0,
                consumerTag, 0, connection.getServerCnx(), "", null,
                false, PulsarApi.CommandSubscribe.InitialPosition.Latest,
                null, this, consumerTag, queueName, ack);
            subscription.addConsumer(consumer);
            consumer.handleFlow(10000);
            tag2ConsumersMap.put(consumerTag, consumer);
        } catch (Exception e) {
            throw e;
        }
        return consumer;
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] BasicCancel[ consumerTag: {}  noWait: {} ]", channelId, consumerTag, noWait);
        }

        unsubscribeConsumer(AMQShortString.toString(consumerTag));
        if (!noWait) {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            BasicCancelOkBody cancelOkBody = methodRegistry.createBasicCancelOkBody(consumerTag);
            connection.writeFrame(cancelOkBody.generateFrame(channelId));
        }
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchangeName, AMQShortString routingKey, boolean mandatory,
                                    boolean immediate) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicPublish[exchange: {} routingKey: {} mandatory: {} immediate: {}]",
                    channelId, exchangeName, routingKey, mandatory, immediate);
        }

        if (exchangeName == null || exchangeName.length() == 0) {
            AmqpExchange amqpExchange = ExchangeContainer.
                    getExchange(connection.getNamespaceName(), AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
            AmqpQueue amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), routingKey.toString());

            if (amqpQueue == null) {
                log.error("Queue[{}] is not declared!", routingKey.toString());
                connection.sendConnectionClose(NOT_FOUND, "Exchange or queue not found.", channelId);
                return;
            }

//            if (amqpQueue.getRouter("") == null) {
//                AmqpMessageRouter amqpMessageRouter = new DirectMessageRouter(
//                        AmqpMessageRouter.Type.Direct, routingKey.toString());
//                amqpQueue.bindExchange(amqpExchange, amqpMessageRouter);
//            }
        }
        MessagePublishInfo info = new MessagePublishInfo(exchangeName, immediate, mandatory, routingKey);
        setPublishFrame(info, null);
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        String queueName = AMQShortString.toString(queue);
        PersistentQueue amqpQueue = (PersistentQueue) QueueContainer.
            getQueue(connection.getNamespaceName(), queueName);
        if (amqpQueue == null) {
            log.error("Queue[{}] is not declared!", queueName);
            connection.sendConnectionClose(NOT_FOUND, "Exchange or queue not found.", channelId);
            return;
        }
        Topic topic = amqpQueue.getIndexTopic();
        AmqpConsumer amqpConsumer = fetchConsumerMap.computeIfAbsent(queueName, value -> {

            Subscription subscription = topic.getSubscription(defaultSubscription);
            AmqpConsumer consumer;
            try {
                if (subscription == null) {
                    subscription = topic.createSubscription(defaultSubscription,
                        PulsarApi.CommandSubscribe.InitialPosition.Earliest, false).get();
                }
                consumer = new AmqpPullConsumer(subscription, PulsarApi.CommandSubscribe.SubType.Shared,
                    topic.getName(), 0, 0, "", 0,
                    connection.getServerCnx(), "", null, false,
                    PulsarApi.CommandSubscribe.InitialPosition.Latest, null, this,
                    "", queueName, noAck);
                subscription.addConsumer(consumer);
                consumer.handleFlow(10000);
                return consumer;
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
        MessageFetchContext context = MessageFetchContext.get(this, amqpConsumer);
        context.handleFetch(noAck);
    }

    @Override
    public void receiveChannelFlow(boolean active) {

    }

    @Override
    public void receiveChannelFlowOk(boolean active) {

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

            AmqpQueue amqpQueue = QueueContainer.getQueue(connection.getNamespaceName(), routingKey);
            AmqpExchange amqpExchange;
            if (exchangeName == null || exchangeName.length() == 0) {
                if (amqpQueue.getDurable()) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE;
                }
            }
            amqpExchange = ExchangeContainer.getExchange(connection.getNamespaceName(), exchangeName);
            connection.getPulsarService().getOrderedExecutor().executeOrdered(amqpExchange, SafeRunnable.safeRun(() -> {
                if (amqpExchange == null) {
                    log.error("publish message error amqpExchange is null.");
                    return;
                }
                CompletableFuture<Position> position = amqpExchange.writeMessageAsync(message, routingKey);
                position.whenComplete((position1, throwable) -> {
                    if (throwable == null) {
                        if (log.isDebugEnabled()) {
                            log.debug("publish message success position {}", position1.toString());
                        }
                        if (confirmOnPublish) {
                            confirmedMessageCounter++;
                            recordFuture(Futures.immediateFuture(null),
                                new ServerTransaction.Action() {
                                    private final long deliveryTag = confirmedMessageCounter;

                                    @Override
                                    public void postCommit() {
                                        BasicAckBody body = connection.getMethodRegistry()
                                            .createBasicAckBody(
                                                deliveryTag, false);
                                        connection.writeFrame(body.generateFrame(channelId));
                                    }

                                    @Override
                                    public void onRollback() {
                                        final BasicNackBody body = new BasicNackBody(deliveryTag,
                                            false,
                                            false);
                                        connection.writeFrame(new AMQFrame(channelId, body));
                                    }
                                });
                        }

                    } else {
                        log.error("publish message error {}", throwable.getMessage());
                    }
                });

            }));

        }
    }

    public void recordFuture(final ListenableFuture<Void> future, final ServerTransaction.Action action) {
        unfinishedCommandsQueue.add(new AsyncCommand(future, action));
    }

    private void sync() {
        if (log.isDebugEnabled()) {
            log.debug("sync() called on channel " + debugIdentity());
        }

        AsyncCommand cmd;
        while ((cmd = unfinishedCommandsQueue.poll()) != null) {
            cmd.complete();
        }
    }

    private final String id = "(" + System.identityHashCode(this) + ")";

    private String debugIdentity() {
        return channelId + id;
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
        if (!ackedMessages.isEmpty() && requeue) {
            Map<AmqpConsumer, List<PositionImpl>> positionMap = new HashMap<>();
            ackedMessages.stream().forEach(association -> {
                AmqpConsumer consumer = association.getConsumer();
                List<PositionImpl> positions = positionMap.computeIfAbsent(consumer,
                        list -> new ArrayList<>());
                positions.add((PositionImpl) association.getPosition());
            });
            positionMap.entrySet().stream().forEach(entry -> {
                entry.getKey().redeliverAmqpMessages(entry.getValue());
            });
        }
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[ {} ] BasicAck[deliveryTag: {} multiple: {} ]", channelId, deliveryTag, multiple);
        }
        messageAck(deliveryTag, multiple);
    }

    public void messageAck(long deliveryTag, boolean multiple) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
                unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            ackedMessages.stream().forEach(entry -> {
                entry.getConsumer().messagesAck(entry.getPosition());
            });
        }

    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {

    }

    @Override
    public void receiveTxSelect() {

    }

    @Override
    public void receiveTxCommit() {

    }

    @Override
    public void receiveTxRollback() {

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
        sync();
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

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    private void closeChannel(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public long getNextDeliveryTag() {
        return ++deliveryTag;
    }

    private int getNextConsumerTag() {
        return ++consumerTag;
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
            consumer.getSubscription().doUnsubscribe(consumer);
            return true;
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
            tag2ConsumersMap.entrySet().stream().forEach(entry -> {
                Consumer consumer = entry.getValue();
                try {
                    consumer.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }

            });
            tag2ConsumersMap.clear();
            fetchConsumerMap.entrySet().stream().forEach(entry -> {
                Consumer consumer = entry.getValue();
                try {
                    consumer.close();
                } catch (BrokerServiceException e) {
                    log.error(e.getMessage());
                }
            });
            fetchConsumerMap.clear();
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

    private void setDefaultQueue(AmqpQueue queue) {
        defaultQueue = queue;
    }

    private AmqpQueue getDefaultQueue() {
        return defaultQueue;
    }

    private void checkExclusiveQueue(AmqpQueue amqpQueue){
        if (amqpQueue != null && amqpQueue.isExclusive()
                && (amqpQueue.getConnectionId() != connection.getConnectionId())) {
            closeChannel(ErrorCodes.ALREADY_EXISTS,
                    "Exclusive queue can not be used form other connection, queueName: '" + amqpQueue.getName() + "'");
        }
    }

    @VisibleForTesting
    public Map<String, Consumer> getTag2ConsumersMap() {
        return tag2ConsumersMap;
    }

    public void restoreCredit(final int count, final long size) {
        if (creditManager == null) {
            return;
        }
        creditManager.restoreCredit(count, size);
    }

    public void useCreditForMessage(final long msgSize) {
        if (creditManager == null) {
            return;
        }
        creditManager.useCreditForMessage(msgSize);
    }

    private void setCredit(final long prefetchSize, final int prefetchCount) {
        if (creditManager == null) {
            creditManager = new AmqpFlowCreditManager(0, 0);
        }
        creditManager.setCreditLimits(prefetchSize, prefetchCount);
    }

    public boolean hasCredit() {
        if (creditManager == null) {
            return true;
        }
        return creditManager.hasCredit();
    }
}
