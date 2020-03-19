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

import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.security.AccessControlException;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.LogMessage;
import org.apache.qpid.server.logging.messages.ChannelMessages;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;


import static org.apache.qpid.server.protocol.v0_8.AMQShortString.createAMQShortString;
import static org.apache.qpid.server.transport.util.Functions.hex;


/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    protected final AmqpConnection connection;

    @Getter
    private final int channelId;

    /**
     * The current message - which may be partial in the sense that not all frames have been received yet - which has
     * been received by this channel. As the frames are received the message gets updated and once all frames have been
     * received the message can then be routed. -- copied qpid
     */
    private IncomingMessage _currentMessage;

    private long _blockTime;
    private long _blockingTimeout;
    private boolean _wireBlockingState;
    private boolean _forceMessageValidation = false;
    private boolean _confirmOnPublish;
    private long _confirmedMessageCounter;

    private ExchangeTopicManager exchangeTopicManager;

    public static final AMQShortString EMPTY_STRING = createAMQShortString((String)null);

    public AmqpChannel(AmqpConnection connection, int channelId) {
        this.connection = connection;
        this.channelId = channelId;
    }

    private void message(final LogMessage message) {
        // TODO - log message
        log.error("FLOW_CONTROL_IGNORED");
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

    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {

    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {

    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
            boolean autoDelete, boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
            boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {

    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {

    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
            FieldTable arguments) {

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

    /**
     * receive BasicPublish command, setPublishFrame
     */
    @Override
    public void receiveBasicPublish(AMQShortString exchangeName, AMQShortString routingKey, boolean mandatory,
            boolean immediate) {
        if(log.isDebugEnabled()) {
            log.debug("RECV[" + channelId + "] BasicPublish[" +" exchange: " + exchangeName +
                    " routingKey: " + routingKey +
                    " mandatory: " + mandatory +
                    " immediate: " + immediate + " ]");
        }

        // TODO - get NamedAddressSpace
        NamedAddressSpace vHost = null;

        if(blockingTimeoutExceeded()) {
            message(ChannelMessages.FLOW_CONTROL_IGNORED());
            closeChannel(ErrorCodes.MESSAGE_TOO_LARGE,
                    "Channel flow control was requested, but not enforced by sender");
        } else {
            MessageDestination destination;

            if (isDefaultExchange(exchangeName)) {
                destination = vHost.getDefaultDestination();
            } else {
                destination = vHost.getAttainedMessageDestination(exchangeName.toString(), true);
            }

            // if the exchange does not exist we raise a channel exception
            if (destination == null) {
                closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange name: '" + exchangeName + "'");
            } else {
                MessagePublishInfo info = new MessagePublishInfo(exchangeName, immediate, mandatory, routingKey);
                try {
                    setPublishFrame(info, destination);
                } catch (AccessControlException e) {
                    connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, e.getMessage(), getChannelId());
                }
            }
        }
    }

    private boolean blockingTimeoutExceeded() {
        return _wireBlockingState && (System.currentTimeMillis() - _blockTime) > _blockingTimeout;
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        _currentMessage = new IncomingMessage(info);
        _currentMessage.setMessageDestination(e);
    }

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
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

    }

    @Override
    public void receiveChannelCloseOk() {

    }

    private boolean hasCurrentMessage() {
        return _currentMessage != null;
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        if(log.isDebugEnabled()) {
            int binaryDataLimit = 2000;
            log.debug("RECV[" + channelId + "] MessageContent[" +" data: " + hex(data, binaryDataLimit) + " ] ");
        }

        if(hasCurrentMessage()) {
            publishContentBody(new ContentBody(data));
        } else {
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentBody(ContentBody contentBody) {
        if (log.isDebugEnabled()) {
            log.debug(debugIdentity() + " content body received on channel " + channelId);
        }

        try {
            long currentSize = _currentMessage.addContentBodyFrame(contentBody);
            if(currentSize > _currentMessage.getSize()) {
                connection.sendConnectionClose(ErrorCodes.FRAME_ERROR,
                        "More message data received than content header defined", channelId);
            } else {
                deliverCurrentMessageIfComplete();
            }
        } catch (RuntimeException e) {
            // we want to make sure we don't keep a reference to the message in the
            // event of an error
            _currentMessage = null;
            throw e;
        }
    }

    private final String id = "(" + System.identityHashCode(this) + ")";

    private String debugIdentity() {
        return channelId + id;
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        if(log.isDebugEnabled()) {
            log.debug("RECV[" + channelId + "] MessageHeader[ properties: {" + properties + "} bodySize: " + bodySize + " ]");
        }

        // TODO - maxMessageSize ?
        long maxMessageSize = 1024 * 1024 * 10;
        if(hasCurrentMessage()) {
            if(bodySize > maxMessageSize) {
                properties.dispose();
                closeChannel(ErrorCodes.MESSAGE_TOO_LARGE,
                        "Message size of " + bodySize + " greater than allowed maximum of " + maxMessageSize);
            } else {
                if (!_forceMessageValidation || properties.checkValid()) {
                    publishContentHeader(new ContentHeaderBody(properties, bodySize));
                } else {
                    properties.dispose();
                    connection.sendConnectionClose(ErrorCodes.FRAME_ERROR,
                            "Attempt to send a malformed content header", channelId);
                }
            }
        } else {
            properties.dispose();
            connection.sendConnectionClose(ErrorCodes.COMMAND_INVALID,
                    "Attempt to send a content header without first sending a publish frame", channelId);
        }
    }

    private void publishContentHeader(ContentHeaderBody contentHeaderBody) {
        if (log.isDebugEnabled()) {
            log.debug("Content header received on channel " + channelId);
        }

        _currentMessage.setContentHeaderBody(contentHeaderBody);

        deliverCurrentMessageIfComplete();
    }

    private void deliverCurrentMessageIfComplete() {
        if (_currentMessage.allContentReceived()) {
            MessagePublishInfo info = _currentMessage.getMessagePublishInfo();
            String routingKey = AMQShortString.toString(info.getRoutingKey());
            String exchangeName = AMQShortString.toString(info.getExchange());

            try {
                MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(_currentMessage);
                // TODO send message to pulsar topic
            } finally {
                _currentMessage = null;
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


    private void closeChannel(int cause, final String message) {
        // TODO - close channel write frame
    }

}
