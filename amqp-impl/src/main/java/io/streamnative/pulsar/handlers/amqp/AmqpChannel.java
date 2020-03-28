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
import static org.apache.qpid.server.transport.util.Functions.hex;
import com.google.common.annotations.VisibleForTesting;
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
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.api.proto.PulsarApi;
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

    private ExchangeTopicManager exchangeTopicManager;
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
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag,
        boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {

        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicConsume[queue:{} consumerTag:{} noLocal:{} noAck:{} exclusive:{} nowait:{}"
                + "arguments:{}]", channelId, queue, consumerTag, noLocal, noAck, arguments);
        }

        String queueName = AMQShortString.toString(queue);
        // TODO Temporarily treat queue as exchange
        connection.getExchangeTopicManager()
            .getTopic(queueName)
            .whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    closeChannel(ErrorCodes.NOT_FOUND, "No such queue, '" + queueName + "'");
                } else {
                    try {
                        String consumerTag1 = subscirbe(AMQShortString.toString(consumerTag),
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

    private String subscirbe(String consumerTag, Topic topic, boolean ack,
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
                entry.getKey().redeliverAMQMessages(entry.getValue());
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
        // TODO - close channel write frame
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
