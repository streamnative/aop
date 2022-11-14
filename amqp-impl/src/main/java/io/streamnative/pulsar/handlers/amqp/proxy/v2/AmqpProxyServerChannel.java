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
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
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

    public AmqpProxyServerChannel(Integer channelId, ProxyClientConnection proxyConnection) {
        this.channelId = channelId;
        this.proxyConnection = proxyConnection;
    }

    public CompletableFuture<ProxyBrokerConnection> getConn(String topic) {
        return proxyConnection.getBrokerConnection(topic);
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive access request.");
        }
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
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
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
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
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, boolean nowait, FieldTable arguments) {
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
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, FieldTable arguments) {
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
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic consume request, queue: {}, consumerTag: {}, noLocal: {}, "
                    + "noAck: {}, exclusive: {}, nowait: {}, arguments: {}.",
                    queue, consumerTag, noLocal, noAck, exclusive, noAck, arguments);
        }
        getConn(queue.toString()).thenAccept(conn -> {
            initChannelIfNeeded(conn);
            BasicConsumeBody basicConsumeBody =
                    new BasicConsumeBody(0, queue, consumerTag, noLocal, noAck, exclusive, nowait, arguments);
            conn.getChannel().writeAndFlush(basicConsumeBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive basic cancel request, consumerTag: {}, noWait: {}.",
                    consumerTag, noWait);
        }
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory, boolean immediate) {
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
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyServerChannel receive channel close request, replyCode: {}, replyText: {}, classId: {}, "
                            + "methodId: {}.", replyCode, replyText, classId, methodId);
        }
        proxyConnection.getConnectionMap().values().forEach(conn -> {
            if (conn.getChannelState().getOrDefault(channelId, null)
                    == ProxyBrokerConnection.ChannelState.OPEN) {
                conn.getChannel().writeAndFlush(
                        new ChannelCloseBody(replyCode, replyText, classId, methodId).generateFrame(channelId));
            }
        });
    }

    @Override
    public void receiveChannelCloseOk() {
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
            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                proxyConnection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.");
                return;
            }
            boolean createIfMissing = false;
            String exchangeType = null;
//            if (isDefaultExchange(AMQShortString.valueOf(exchangeName))
//                    || isBuildInExchange(exchangeName)) {
//                // Auto create default and buildIn exchanges if use.
//                createIfMissing = true;
//                exchangeType = getExchangeType(exchangeName);
//            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
            }

            String msg = new String(message.getData());
            final String exName = exchangeName;
            proxyConnection.getProxyServer()
                    .getProducer("persistent://public/" + proxyConnection.getVhost() + "/__amqp_exchange__" + exchangeName)
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
