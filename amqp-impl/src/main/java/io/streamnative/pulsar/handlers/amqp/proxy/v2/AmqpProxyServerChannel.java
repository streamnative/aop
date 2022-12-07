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
package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import static org.apache.qpid.server.protocol.ErrorCodes.COMMAND_INVALID;
import static org.apache.qpid.server.protocol.ErrorCodes.FRAME_ERROR;
import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;
import static org.apache.qpid.server.protocol.ErrorCodes.MESSAGE_TOO_LARGE;
import static org.apache.qpid.server.transport.util.Functions.hex;

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageData;
import io.streamnative.pulsar.handlers.amqp.AmqpOutputConverter;
import io.streamnative.pulsar.handlers.amqp.flow.AmqpFlowCreditManager;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

@Slf4j
public class AmqpProxyServerChannel implements ServerChannelMethodProcessor {

    private final Integer channelId;
    private final ProxyClientConnection proxyConnection;
    private boolean isInit;

    private IncomingMessage currentMessage;
    private final AmqpOutputConverter outputConverter;

    private final Map<String, CompletableFuture<Producer<byte[]>>> producerMap;
    private final List<CompletableFuture<AmqpConsumer>> consumerList;

    private final AtomicLong deliveryTag = new AtomicLong();
    private final UnacknowledgedMessageMap unacknowledgedMessageMap;
    private final AmqpFlowCreditManager creditManager;

    public AmqpProxyServerChannel(Integer channelId, ProxyClientConnection proxyConnection) {
        this.channelId = channelId;
        this.proxyConnection = proxyConnection;
        this.outputConverter = new AmqpOutputConverter(this.proxyConnection.getCtx().channel());
        this.producerMap = new ConcurrentHashMap<>();
        this.consumerList = new ArrayList<>();
        this.unacknowledgedMessageMap = new UnacknowledgedMessageMap(this);
        this.creditManager = new AmqpFlowCreditManager(0, 0);
    }

    public CompletableFuture<ProxyBrokerConnection> getConn(String topic) {
        return proxyConnection.getBrokerConnection(topic);
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active,
                                     boolean write, boolean read) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive access request.");
        }
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable,
                                       boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive exchange declare request, exchange: {}, type: {}, passive: {},"
                            + "durable: {}, autoDelete: {}, internal: {}, nowait: {}, arguments: {}.",
                    exchange, type, passive, durable, autoDelete, internal, nowait, arguments);
        }
        getConn(exchange.toString()).thenAccept(conn -> {
            initChannelIfNeeded(conn);
            ExchangeDeclareBody exchangeDeclareBody = new ExchangeDeclareBody(
                    0, exchange, type, passive, durable, autoDelete, internal, nowait, arguments);
            conn.getChannel().writeAndFlush(exchangeDeclareBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive exchange delete request, exchange: {}, ifUnused: {}, nowait: {}.",
                    exchange, ifUnused, nowait);
        }
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive exchange bound request, exchange: {}, routingKey: {}, queue: {}.",
                    exchange, routingKey, queue);
        }
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                                    boolean autoDelete, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive queue declare request, queue: {}, passive: {},"
                            + "durable: {}, exclusive: {}, autoDelete: {}, nowait: {}, arguments: {}.",
                    queue, passive, durable, exclusive, autoDelete, nowait, arguments);
        }
        getConn(queue.toString()).thenAccept(conn -> {
            initChannelIfNeeded(conn);
            log.info("[ProxyServerChannel] write receiveExchangeDeclare frame");
            QueueDeclareBody queueDeclareBody = new QueueDeclareBody(
                    0, queue, passive, durable, exclusive, autoDelete, nowait, arguments);
            conn.getChannel().writeAndFlush(queueDeclareBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                 boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive queue bind request, queue: {}, exchange: {}, bindingKey: {}, "
                            + "nowait: {}, arguments: {}.", queue, exchange, bindingKey, nowait, arguments);
        }
        getConn(exchange.toString()).thenAccept(conn -> {
            initChannelIfNeeded(conn);
            QueueBindBody queueBindBody = new QueueBindBody(0, queue, exchange, bindingKey, nowait, arguments);
            conn.getChannel().writeAndFlush(queueBindBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive queue purge request, queue: {}, nowait: {}.", queue, nowait);
        }
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive queue purge request, queue: {}, ifUnused: {}, nowait: {}.",
                    queue, ifUnused, nowait);
        }
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                   FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive queue unbind request, queue: {}, exchange: {}, bindingKey: {}, "
                            + "arguments: {}.", queue, exchange, bindingKey, arguments);
        }
        getConn(exchange.toString()).thenAccept(conn -> {
            initChannelIfNeeded(conn);
            QueueUnbindBody command = new QueueUnbindBody(0, queue, exchange, bindingKey, arguments);
            conn.getChannel().writeAndFlush(command.generateFrame(channelId));
        });
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic recover request, requeue: {}, sync: {}.", requeue, sync);
        }
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic recover request, prefetchSize: {}, prefetchCount: {}, "
                    + "global: {}.", prefetchSize, prefetchCount, global);
        }
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck,
                                    boolean exclusive, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic consume request, queue: {}, consumerTag: {}, noLocal: {}, "
                    + "noAck: {}, exclusive: {}, nowait: {}, arguments: {}.",
                    queue, consumerTag, noLocal, noAck, exclusive, noAck, arguments);
        }
        String topic = "persistent://public/" + proxyConnection.getVhost() + "/__amqp_queue__" + queue;
        String finalConsumerTag = getConsumerTag(consumerTag);
        getConsumer(topic, finalConsumerTag, noAck).thenAccept(consumer -> {
            BasicConsumeOkBody basicConsumeOkBody = new BasicConsumeOkBody(AMQShortString.valueOf(finalConsumerTag));
            proxyConnection.getCtx().channel().writeAndFlush(basicConsumeOkBody.generateFrame(channelId));
            new Thread(() -> {
                startToConsume(consumer);
            }).start();
        });
//        getConn(queue.toString()).thenAccept(conn -> {
//            initChannelIfNeeded(conn);
//            BasicConsumeBody basicConsumeBody =
//                    new BasicConsumeBody(0, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
//            conn.getChannel().writeAndFlush(basicConsumeBody.generateFrame(channelId));
//        });
    }

    private String getConsumerTag(AMQShortString consumerTag) {
        if (consumerTag == null) {
            return "aop.ctag-" + proxyConnection.getCtx().channel().remoteAddress() + "-" + UUID.randomUUID();
        } else {
            // don't change the consumer tag if the consumerTag is existing
            return consumerTag.toString();
        }
    }

    private void startToConsume(AmqpConsumer consumer) {
        MessageImpl<byte[]> message = null;
        while (true) {
            try {
                message = (MessageImpl<byte[]>) consumer.getConsumer().receive();
                MessageId messageId = message.getMessageId();
                long deliveryIndex = deliveryTag.incrementAndGet();
                outputConverter.writeDeliver(
                        MessageConvertUtils.messageToAmqpBody(message),
                        channelId,
                        false,
                        deliveryIndex,
                        AMQShortString.createAMQShortString(consumer.getConsumerTag()));
                if (consumer.isAutoAck()) {
//                    log.info("ack message {} for topic {} by auto ack.", messageId, consumer.getTopic());
                    consumer.getConsumer().acknowledgeAsync(messageId).exceptionally(t -> {
                        log.error("Failed to ack message {} for topic {} by auto ack.",
                                messageId, consumer.getTopic(), t);
                        return null;
                    });
                } else {
                    unacknowledgedMessageMap.add(deliveryIndex, messageId, consumer, 1);
                }
            } catch (Exception e) {
                log.error("Failed to send message", e);
                throw new RuntimeException(e);
            } finally {
                if (message != null) {
                    message.getDataBuffer().release();
                }
            }
        }
    }

    public CompletableFuture<AmqpConsumer> getConsumer(String topic, String consumerTag, boolean autoAck) {
        PulsarClient client;
        try {
            client = proxyConnection.getProxyServer().getPulsar().getClient();
        } catch (PulsarServerException e) {
            return FutureUtil.failedFuture(e);
        }
        CompletableFuture<AmqpConsumer> consumerFuture = new CompletableFuture<>();
        client.newConsumer()
                .topic(topic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("AMQP_DEFAULT")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName(UUID.randomUUID().toString())
//                .poolMessages(true)
//                .receiverQueueSize(0)
                .subscribeAsync()
                .thenAccept(consumer-> {
                    consumerFuture.complete(new AmqpConsumer(consumer, consumerTag, autoAck));
                    consumerList.add(consumerFuture);
                })
                .exceptionally(t -> {
                    consumerFuture.completeExceptionally(t);
                    return null;
                });
        return consumerFuture;
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic cancel request, consumerTag: {}, noWait: {}.",
                    consumerTag, noWait);
        }
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
                                    boolean immediate) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic publish request, exchange: {}, routingKey: {}, mandatory: {}, "
                            + "immediate: {}.", exchange, routingKey, mandatory, immediate);
        }
        MessagePublishInfo info = new MessagePublishInfo(exchange, immediate,
                mandatory, routingKey);
        setPublishFrame(info, null);
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic get request, queue: {}, noAck: {}.", queue, noAck);
        }
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic ack request, deliveryTag: {}, multiple: {}.",
                    deliveryTag, multiple);
        }
        messageAck(deliveryTag, multiple);
    }

    private void messageAck(long deliveryTag, boolean multiple) {
        Collection<UnacknowledgedMessageMap.MessageConsumerAssociation> ackedMessages =
                unacknowledgedMessageMap.acknowledge(deliveryTag, multiple);
        if (!ackedMessages.isEmpty()) {
            ackedMessages.forEach(entry -> {
                entry.getConsumer().acknowledgeAsync(entry.getMessageId()).exceptionally(t -> {
                    log.error("Failed to ack message with delivery tag {}(messageId: {}).",
                            deliveryTag, entry.getMessageId(), t);
                    return null;
                });
            });
        } else {
            // TODO close channel
        }
    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic reject request, deliveryTag: {}, requeue: {}.",
                    deliveryTag, requeue);
        }
    }

    @Override
    public void receiveTxSelect() {
        // nothing to do
    }

    @Override
    public void receiveTxCommit() {
        // nothing to do
    }

    @Override
    public void receiveTxRollback() {
        // nothing to do
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive confirm select request, nowait: {}.", nowait);
        }
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive channel flow request, active: {}.", active);
        }
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        // nothing to do
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive channel close request, replyCode: {}, replyText: {}, classId: {}, "
                            + "methodId: {}.", replyCode, replyText, classId, methodId);
        }
        close();
        proxyConnection.getConnectionMap().values().forEach(conn -> {
            if (conn.getChannelState().getOrDefault(channelId, null)
                    == ProxyBrokerConnection.ChannelState.OPEN) {
                conn.getChannel().writeAndFlush(
                        new ChannelCloseBody(replyCode, replyText, classId, methodId).generateFrame(channelId));
            }
        });
    }

    protected void close() {
        log.info("cleanup resource for channel {}.", channelId);
        for (CompletableFuture<Producer<byte[]>> producerFuture : producerMap.values()) {
            producerFuture.thenAccept(Producer::closeAsync);
        }
        for (CompletableFuture<AmqpConsumer> consumerFuture : consumerList) {
            consumerFuture.thenAccept(amqpConsumer -> amqpConsumer.getConsumer().closeAsync());
        }
    }

    @Override
    public void receiveChannelCloseOk() {
        // nothing to do
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
            proxyConnection.sendConnectionClose(COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame.");
        }
    }

    private void publishContentBody(ContentBody contentBody) {
        if (log.isDebugEnabled()) {
            log.debug("content body received on channel {}", channelId);
        }

        try {
            long currentSize = currentMessage.addContentBodyFrame(contentBody);
            if (currentSize > currentMessage.getSize()) {
                proxyConnection.sendConnectionClose(FRAME_ERROR,
                        "More message data received than content header defined");
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
                proxyConnection.sendChannelClose(channelId, MESSAGE_TOO_LARGE,
                        "Message size of " + bodySize + " greater than allowed maximum of " + maxMessageSize);
            } else {
                publishContentHeader(new ContentHeaderBody(properties, bodySize));
            }
        } else {
            properties.dispose();
            proxyConnection.sendConnectionClose(COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame");
        }
    }

    private boolean hasCurrentMessage() {
        return currentMessage != null;
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
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                proxyConnection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.");
                return;
            }
//            boolean createIfMissing = false;
//            String exchangeType = null;
//            if (isDefaultExchange(AMQShortString.valueOf(exchangeName))
//                    || isBuildInExchange(exchangeName)) {
//                // Auto create default and buildIn exchanges if use.
//                createIfMissing = true;
//                exchangeType = getExchangeType(exchangeName);
//            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
            }

            String topic = "persistent://public/" + proxyConnection.getVhost() + "/__amqp_exchange__" + exchangeName;
            getProducer(topic)
                    .thenCompose(producer -> {
                        return producer.newMessage()
                                .value(message.getData())
                                .properties(message.getProperties())
                                .sendAsync();
                    })
                    .thenAccept(position -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Publish message success, position {}", position.toString());
                        }
                    })
                    .exceptionally(t -> {
                        log.error("Failed to write message to exchange", t);
                        return null;
                    });
        }
    }

    public CompletableFuture<Producer<byte[]>> getProducer(String topic) {
        PulsarClient client;
        try {
            client = proxyConnection.getProxyServer().getPulsar().getClient();
        } catch (PulsarServerException e) {
            return FutureUtil.failedFuture(e);
        }
        return producerMap.computeIfAbsent(topic, k -> {
            CompletableFuture<Producer<byte[]>> producerFuture = new CompletableFuture<>();
            client.newProducer()
                    .topic(topic)
                    .enableBatching(false)
                    .createAsync()
                    .thenAccept(producerFuture::complete)
                    .exceptionally(t -> {
                        producerFuture.completeExceptionally(t);
                        producerMap.remove(topic, producerFuture);
                        return null;
                    });
            return producerFuture;
        });
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel ignore all but close ok.");
        }
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic nack request, deliveryTag: {}, multiple: {}, requeue: {}.",
                    deliveryTag, multiple, requeue);
        }
    }

    public void initChannelIfNeeded(ProxyBrokerConnection coon) {
        if (!isInit) {
            ChannelOpenBody channelOpenBody = new ChannelOpenBody();
            coon.getChannel().writeAndFlush(channelOpenBody.generateFrame(channelId));
        }
    }

    public void initComplete() {
        isInit = true;
    }

}
