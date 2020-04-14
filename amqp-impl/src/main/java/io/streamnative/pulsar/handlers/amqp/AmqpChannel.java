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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.log4j.Log4j2;
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
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;


/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    private final int channelId;
    private final AmqpConnection connection;
    private final AtomicBoolean blocking = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);
    private long confirmedMessageCounter;
    private boolean confirmOnPublish;

    private final UnacknowledgedMessageMap unacknowledgedMessageMap;

    /** Maps from consumer tag to consumers instance. */
    private final Map<String, Consumer> tag2ConsumersMap = new ConcurrentHashMap<>();
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

    public AmqpChannel(int channelId, AmqpConnection connection) {
        this.channelId = channelId;
        this.connection = connection;
        this.amqpTopicManager = connection.getAmqpTopicManager();
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap();
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
                            exchange.toString(), AmqpExchange.Type.value(type.toString()));
                    connection.putExchange(exchange.toString(), inMemoryExchange);
                }

                // if declare a default exchange, return success.
                connection.writeFrame(declareOkBody.generateFrame(channelId));
            }
        } else {
            String name = exchange.toString();

            // create new exchange, on first step, we just create a Pulsar Topic.
            if (PulsarService.State.Started == connection.getPulsarService().getState()) {

                if (!durable) {
                    // in-memory integration
                    InMemoryExchange inMemoryExchange = new InMemoryExchange(
                            name, AmqpExchange.Type.value(type.toString()));
                    connection.putExchange(name, inMemoryExchange);
                    connection.writeFrame(declareOkBody.generateFrame(channelId));
                    return;
                }

                TopicName topicName = TopicName.get(
                        TopicDomain.persistent.value(), connection.getNamespaceName(), name);
                try {
                    PersistentTopic persistentTopic = amqpTopicManager.getTopic(topicName.toString()).get();
                    if (persistentTopic == null) {
                        connection.sendConnectionClose(INTERNAL_ERROR, "AOP Create Exchange failed.", channelId);
                        return;
                    }
                    connection.putExchange(name, new PersistentExchange(
                            name, AmqpExchange.Type.value(type.toString()), persistentTopic));
                    connection.writeFrame(declareOkBody.generateFrame(channelId));
                } catch (Exception e) {
                    log.error(channelId + "Exchange declare failed! exchangeName: " + name, e);
                    connection.sendConnectionClose(INTERNAL_ERROR, "AOP Create Exchange failed.", channelId);
                }
            } else {
                connection.sendConnectionClose(INTERNAL_ERROR, "PulsarService not start.", channelId);
            }
        }
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                    nowait);
        }
        if (isDefaultExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Default Exchange cannot be deleted. ", channelId);
        } else {
            TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                    connection.getNamespaceName(), exchange.toString());

            Topic topic = amqpTopicManager.getOrCreateTopic(topicName.toString(), false);
            if (null == topic) {
                closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchange + "'");
            } else {
                if (ifUnused && topic.getSubscriptions().isEmpty()) {
                    closeChannel(ErrorCodes.IN_USE, "Exchange has bindings. ");
                } else {
                    try {
                        topic.delete().get();
                        ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                        connection.writeFrame(responseBody.generateFrame(channelId));
                    } catch (Exception e) {
                        connection.sendConnectionClose(INTERNAL_ERROR,
                                "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
                    }
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

        AmqpQueue amqpQueue;
        if (!durable) {
            // in-memory integration
            amqpQueue = new InMemoryQueue(queue.toString());
        } else {
            try {
                PersistentTopic indexTopic = amqpTopicManager.getTopic(
                            PersistentQueue.getIndexTopicName(connection.getNamespaceName(), queue.toString())).get();
                amqpQueue = new PersistentQueue(queue.toString(), indexTopic);
            } catch (ExecutionException | InterruptedException e) {
                log.error(channelId + "Exchange declare failed! queueName: {}", queue.toString());
                connection.sendConnectionClose(INTERNAL_ERROR, "AOP Create Exchange failed.", channelId);
                return;
            }
        }
        connection.putQueue(queue.toString(), amqpQueue);

        // return success.
        // when call QueueBind, then create Pulsar sub.
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
        connection.writeFrame(responseBody.generateFrame(channelId));
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

        AmqpQueue amqpQueue = connection.getQueue(queue.toString());
        AmqpExchange amqpExchange = connection.getExchange(exchange.toString());

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
        AmqpQueue amqpQueue = connection.getQueue(queue.toString());
        AmqpExchange amqpExchange = connection.getExchange(exchange.toString());
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
                topic.getSubscription(queue.toString()).delete().get();
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

    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag,
        boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {

        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicConsume[queue:{} consumerTag:{} noLocal:{} noAck:{} exclusive:{} nowait:{}"
                + "arguments:{}]", channelId, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
        }

        // in-memory integration
        AmqpQueue amqpQueue = connection.getQueue(queue.toString());
        if (amqpQueue instanceof InMemoryQueue) {
            try {
                final String consumerTag1;
                if (consumerTag == null) {
                    consumerTag1 = "consumerTag" + getNextConsumerTag();
                } else {
                    consumerTag1 = consumerTag.toString();
                }
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
        }

        String queueName = AMQShortString.toString(queue);
        // TODO Temporarily treat queue as exchange
        connection.getAmqpTopicManager()
            .getTopic(queueName)
            .whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue, '" + queueName + "'");
                } else {
                    try {
                        String consumerTag1 = subscribe(AMQShortString.toString(consumerTag),
                            topic, noAck, arguments, exclusive, noLocal);
                        if (!nowait) {
                            MethodRegistry methodRegistry = connection.getMethodRegistry();
                            AMQMethodBody responseBody = methodRegistry.
                                createBasicConsumeOkBody(AMQShortString.createAMQShortString(consumerTag1));
                            connection.writeFrame(responseBody.generateFrame(channelId));
                        }
                    } catch (Exception e) {
                        closeChannel(ErrorCodes.SYNTAX_ERROR, e.getMessage());
                    }
                }
            });
    }

    private String subscribe(String consumerTag, Topic topic, boolean ack,
        FieldTable arguments, boolean exclusive, boolean noLocal) throws ConsumerTagInUseException,
        InterruptedException, ExecutionException, BrokerServiceException {
        if (consumerTag == null) {
            consumerTag = "consumerTag" + getNextConsumerTag();
        }

        if (tag2ConsumersMap.containsKey(consumerTag)) {
            throw new ConsumerTagInUseException("Consumer already exists with same consumerTag: " + consumerTag);
        }
        Subscription subscription = topic.getSubscription(defaultSubscription);
        try {
            if (subscription == null) {
                subscription = topic.createSubscription(defaultSubscription,
                    PulsarApi.CommandSubscribe.InitialPosition.Earliest, false).get();
            }
            Consumer consumer =
                new AmqpConsumer(subscription, exclusive ? PulsarApi.CommandSubscribe.SubType.Exclusive :
                    PulsarApi.CommandSubscribe.SubType.Shared, topic.getName(), 0, 0,
                    consumerTag, 0, connection.getServerCnx(), "", null,
                    false, PulsarApi.CommandSubscribe.InitialPosition.Earliest,
                null, this, defaultSubscription, ack);
            subscription.addConsumer(consumer);
            tag2ConsumersMap.put(consumerTag, consumer);
        } catch (Exception e) {
            throw e;
        }
        return consumerTag;
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
            AmqpExchange amqpExchange = connection.getExchange(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
            AmqpQueue amqpQueue = connection.getQueue(routingKey.toString());

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

            AmqpQueue amqpQueue = connection.getQueue(routingKey);
            AmqpExchange amqpExchange;
            if (exchangeName == null || exchangeName.length() == 0) {
                if (amqpQueue.getDurable()) {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
                } else {
                    exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE;
                }
            }
            amqpExchange = connection.getExchange(exchangeName);

            amqpExchange.writeMessageAsync(message, routingKey);
            BasicAckBody basicAckBody = connection.getMethodRegistry().createBasicAckBody(deliveryTag, false);
            connection.writeFrame(basicAckBody.generateFrame(channelId));
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
            Map<Subscription, List<Position>> positionMap = new HashMap<>();
            ackedMessages.stream().forEach(association -> {
                Subscription subscription = association.getConsumer().getSubscription();
                List<Position> positions = positionMap.computeIfAbsent(subscription,
                    list -> new ArrayList<>());
                positions.add(association.getPosition());
            });
            positionMap.entrySet().stream().forEach(entry -> {
                entry.getKey().acknowledgeMessage(entry.getValue(),
                    PulsarApi.CommandAck.AckType.Individual, null);
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

    }

    public void receivedComplete() {
        processAsync();
    }

    private void sendChannelClose(int cause, final String message) {
        connection.closeChannelAndWriteFrame(this, cause, message);
    }

    public void processAsync() {

    }

    public void close() {
        // TODO
        unsubscribeConsumerAll();
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
        tag2ConsumersMap.entrySet().stream().forEach(entry -> {
            Consumer consumer = entry.getValue();
            consumer.getSubscription().doUnsubscribe(consumer);
        });
        tag2ConsumersMap.clear();
    }

    @VisibleForTesting
    public Map<String, Consumer> getTag2ConsumersMap() {
        return tag2ConsumersMap;
    }
}
