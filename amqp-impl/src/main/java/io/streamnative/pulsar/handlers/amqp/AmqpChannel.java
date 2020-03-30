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

import static org.apache.qpid.server.protocol.v0_8.AMQShortString.createAMQShortString;
import static org.apache.qpid.server.transport.util.Functions.hex;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
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

    /**
     * The current message - which may be partial in the sense that not all frames have been received yet - which has
     * been received by this channel. As the frames are received the message gets updated and once all frames have been
     * received the message can then be routed.
     */
    private IncomingMessage currentMessage;

    private ExchangeTopicManager exchangeTopicManager;

    public static final AMQShortString EMPTY_STRING = createAMQShortString((String) null);

    public AmqpChannel(int channelId, AmqpConnection connection) {
        this.channelId = channelId;
        this.connection = connection;
        this.exchangeTopicManager = connection.getExchangeTopicManager();
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
                // if declare a default exchange, return success.
                connection.writeFrame(declareOkBody.generateFrame(channelId));
            }
        } else {
            String name = exchange.toString();

            // create new exchange, on first step, we just create a Pulsar Topic.
            if (PulsarService.State.Started == connection.getPulsarService().getState()) {
                TopicName topicName;
                if (durable) {
                    topicName = TopicName.get(TopicDomain.persistent.value(), connection.getNamespaceName(), name);
                } else {
                    topicName = TopicName.get(TopicDomain.non_persistent.value(), connection.getNamespaceName(), name);
                }
                Topic topic = exchangeTopicManager.getOrCreateTopic(topicName.toString(), true);
                if (null == topic) {
                    connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "AOP Create Exchange failed.", channelId);
                } else {
                    connection.writeFrame(declareOkBody.generateFrame(channelId));
                }
            } else {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "PulsarService not start.", channelId);
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

            Topic topic = exchangeTopicManager.getOrCreateTopic(topicName.toString(), false);
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
                        connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
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

        Topic topic = exchangeTopicManager.getOrCreateTopic(topicName.toString(), false);
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
                .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(replyText.toString()));
        connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
        boolean autoDelete, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "RECV[{}] QueueDeclare[ queue: {}, passive: {}, durable:{}, "
                            + "exclusive:{}, autoDelete:{}, nowait:{}, arguments:{} ]",
                    channelId, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        // return success.
        // when call QueueBind, then create Pulsar sub.
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(queue, 0, 0);
        connection.writeFrame(responseBody.generateFrame(channelId));
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueBind[ queue: {}, exchange: {}, bindingKey:{}, nowait:{}, arguments:{} ]",
                    channelId, exchange, bindingKey, nowait, arguments);
        }
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                connection.getNamespaceName(), exchange.toString());

        Topic topic = exchangeTopicManager.getOrCreateTopic(topicName.toString(), false);
        if (null == topic) {
            closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchange + "'");
        } else {
            // create a new sub to Pulsar Topic(exchange in AMQP)
            try {
                topic.createSubscription(queue.toString(),
                        PulsarApi.CommandSubscribe.InitialPosition.Earliest, false).get();
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            } catch (Exception e) {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
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
        TopicName topicName = TopicName.get(TopicDomain.persistent.value(),
                connection.getNamespaceName(), exchange.toString());

        Topic topic = exchangeTopicManager.getOrCreateTopic(topicName.toString(), false);
        if (null == topic) {
            connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "exchange not found.", channelId);
        } else {
            try {
                topic.getSubscription(queue.toString()).delete().get();
                final AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
                connection.writeFrame(responseBody.generateFrame(channelId));
            } catch (Exception e) {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "unbind failed:" + e.getMessage(), channelId);
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
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck,
        boolean exclusive, boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {

    }

    @Override
    public void receiveBasicPublish(AMQShortString exchangeName, AMQShortString routingKey, boolean mandatory,
            boolean immediate) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicPublish[exchange: {} routingKey: {} mandatory: {} immediate: {}]",
                    channelId, exchangeName, routingKey, mandatory, immediate);
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
//            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());

            try {
                // TODO send message to pulsar topic
                connection.getExchangeTopicManager()
                        .getTopic(exchangeName)
                        .whenComplete((mockTopic, throwable) -> {
                            if (throwable != null) {

                            } else {
                                MessagePublishContext.publishMessages(currentMessage, mockTopic);
                                long deliveryTag = 1;
                                BasicAckBody body = connection.getMethodRegistry()
                                        .createBasicAckBody(
                                                deliveryTag, false);
                                connection.writeFrame(body.generateFrame(channelId));
                            }
                });
            } finally {
                currentMessage = null;
            }
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {

    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {

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

}
