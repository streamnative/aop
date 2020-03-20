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

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
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

    public AmqpChannel(int channelId, AmqpConnection connection) {
        this.channelId = channelId;
        this.connection = connection;
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
            // TODO need to associate with VHost/namespace.
            if (PulsarService.State.Started == connection.getPulsarService().getState()) {
                try {
                    if (durable) {
                        // use sync create.
                        connection.getPulsarService().getAdminClient().topics().createNonPartitionedTopic(name);
                    } else {
                        // TODO create nonPersistent Topic for nonDurable Exchange.
                    }
                } catch (PulsarAdminException e) {
                    connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                            "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
                } catch (PulsarServerException e) {
                    connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                            "Catch a PulsarServerException: " + e.getMessage() + ". ", channelId);
                }
                connection.writeFrame(declareOkBody.generateFrame(channelId));
            } else {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR, "PulsarService not start.", channelId);
            }
        }
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

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
        boolean immediate) {

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

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {

    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {

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
}
