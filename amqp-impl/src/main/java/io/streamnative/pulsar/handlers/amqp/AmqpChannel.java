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

import java.util.List;

import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;


/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    protected final AmqpConnection connection;

    private final int channelId;

    public AmqpChannel(AmqpConnection connection, int channelId) {
        this.connection = connection;
        this.channelId = channelId;
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
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                    nowait);
        }
        if (isDefaultExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Default Exchange cannot be deleted", channelId);
        } else {
            final String exchangeName = exchange.toString();
            // TODO get namespace.
            String namespace = "";
            try {
                List<String> topics = connection.getPulsarService().getAdminClient().topics().
                        getList(namespace);
                List<String> queues = connection.getPulsarService().getAdminClient().topics().
                        getSubscriptions(exchangeName);
                if (!topics.contains(exchangeName)) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such exchange: '" + exchange + "'");
                } else {
                    if (ifUnused && null != queues && !queues.isEmpty()) {
                        closeChannel(ErrorCodes.IN_USE, "Exchange has bindings");
                    } else {
                        connection.getPulsarService().getAdminClient().topics().delete(exchangeName);
                        ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().createExchangeDeleteOkBody();
                        connection.writeFrame(responseBody.generateFrame(channelId));
                    }
                }
            } catch (PulsarAdminException e) {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                        "Catch a PulsarAdminException: " + e.getMessage() + ". ", channelId);
            } catch (PulsarServerException e) {
                connection.sendConnectionClose(ErrorCodes.INTERNAL_ERROR,
                        "Catch a PulsarServerException: " + e.getMessage() + ". ", channelId);
            }
        }
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                    routingKey, queue);
        }
        // TODO need to add logic.
        // return success.
        int replyCode = ExchangeBoundOkBody.OK;
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(null));
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
        // TODO
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
        // create a new sub to Pulsar Topic(exchange in AMQP)
        // TODO
        MethodRegistry methodRegistry = connection.getMethodRegistry();
        AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
        connection.writeFrame(responseBody.generateFrame(channelId));
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
        // TODO
        // 1. check queue and exchange is existed?
        // 2. delete the sub mapped to this queue.

        final AMQMethodBody responseBody = connection.getMethodRegistry().createQueueUnbindOkBody();
        connection.writeFrame(responseBody.generateFrame(channelId));
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

    }

    @Override
    public void receiveChannelCloseOk() {

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

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    private void closeChannel(int cause, final String message) {
    }

}
