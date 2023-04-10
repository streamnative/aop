/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp;

import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;
import io.streamnative.pulsar.handlers.amqp.admin.AmqpAdmin;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;

/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpMultiBundlesChannel extends AmqpChannel {

    private final Map<String, Producer<byte[]>> producerMap;
    private final List<AmqpPulsarConsumer> consumerList;

    private final Map<String, MessagePublishInfo> publishInfoMap = new HashMap<>();

    private volatile String defQueue;

    public AmqpMultiBundlesChannel(int channelId, AmqpConnection connection, AmqpBrokerService amqpBrokerService) {
        super(channelId, connection, amqpBrokerService);
        this.producerMap = new ConcurrentHashMap<>();
        this.consumerList = new ArrayList<>();
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

        ExchangeDeclareParams params = new ExchangeDeclareParams();
        params.setType(type != null ? type.toString() : null);
        params.setInternal(internal);
        params.setAutoDelete(autoDelete);
        params.setDurable(durable);
        params.setPassive(passive);
        params.setArguments(FieldTable.convertToMap(arguments));

        getAmqpAdmin().exchangeDeclare(
                connection.getNamespaceName(), exchange.toString(), params).thenAccept(__ -> {
            if (!nowait) {
                connection.writeFrame(
                        connection.getMethodRegistry().createExchangeDeclareOkBody().generateFrame(channelId));
            }
        }).exceptionally(t -> {
            log.error("Failed to declare exchange {} in vhost {}. type: {}, passive: {}, durable: {}, "
                            + "autoDelete: {}, nowait: {}", exchange, connection.getNamespaceName(), type, passive,
                    durable, autoDelete, nowait, t);
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

        QueueDeclareParams params = new QueueDeclareParams();
        params.setDurable(durable);
        params.setExclusive(exclusive);
        params.setAutoDelete(autoDelete);
        params.setPassive(passive);
        params.setArguments(FieldTable.convertToMap(arguments));
        getAmqpAdmin().queueDeclare(
                connection.getNamespaceName(), queue.toString(), params).thenAccept(amqpQueue -> {
            setDefQueue(queue.toString());
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            QueueDeclareOkBody responseBody = methodRegistry.createQueueDeclareOkBody(
                    AMQShortString.createAMQShortString(queue.toString()), 0, 0);
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

        BindingParams params = new BindingParams();
        params.setRoutingKey(bindingKey != null ? bindingKey.toString() : "");
        params.setArguments(FieldTable.convertToMap(argumentsTable));

        AMQShortString finalQueue = getDefQueue(queue);
        getAmqpAdmin().queueBind(connection.getNamespaceName(),
                        exchange.toString(), finalQueue.toString(), params)
                .thenAccept(__ -> {
                    MethodRegistry methodRegistry = connection.getMethodRegistry();
                    AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
                    connection.writeFrame(responseBody.generateFrame(channelId));
                }).exceptionally(t -> {
                    log.error("Failed to bind queue {} to exchange {}.", finalQueue, exchange, t);
                    handleAoPException(t);
                    return null;
                });
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                                   FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueUnbind[ queue: {}, exchange:{}, bindingKey:{}, arguments:{} ]", channelId,
                    queue, exchange, bindingKey, arguments);
        }

        getAmqpAdmin().queueUnbind(connection.getNamespaceName(), exchange.toString(),
                        queue.toString(), bindingKey.toString())
                .thenAccept(__ -> {
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
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] QueueDelete[ queue: {}, ifUnused:{}, ifEmpty:{}, nowait:{} ]", channelId, queue,
                    ifUnused, ifEmpty, nowait);
        }
        Map<String, Object> params = new HashMap<>(4);
        params.put("if-unused", ifUnused);
        params.put("if-empty", ifEmpty);
        params.put("mode", "delete");
        params.put("name", queue.toString());
        params.put("vhost", connection.getNamespaceName().getLocalName());
        getAmqpAdmin().queueDelete(connection.getNamespaceName(), queue.toString(), params)
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

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] receiveExchangeDelete[ exchange: {}, ifUnused:{} ]", channelId,
                    ifUnused, nowait);
        }
        Map<String, Object> params = new HashMap<>(2);
        params.put("if-unused", ifUnused);
        params.put("name", exchange.toString());
        params.put("vhost", connection.getNamespaceName().getLocalName());
        getAmqpAdmin().exchangeDelete(connection.getNamespaceName(), exchange.toString(), params)
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
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicQos[prefetchSize: {} prefetchCount: {} global: {}]",
                    channelId, prefetchSize, prefetchCount, global);
        }

        // ignored this method first
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

        String finalConsumerTag = getConsumerTag(consumerTag);
        getConsumer(queue.toString(), finalConsumerTag, noAck).thenAccept(consumer -> {
            if (!nowait) {
                BasicConsumeOkBody basicConsumeOkBody =
                        new BasicConsumeOkBody(AMQShortString.valueOf(finalConsumerTag));
                connection.writeFrame(basicConsumeOkBody.generateFrame(channelId));
            }
            consumer.startConsume();
        });
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
                                    boolean immediate) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] BasicPublish[exchange: {} routingKey: {} mandatory: {} immediate: {}]", channelId,
                    exchange, routingKey, mandatory, immediate);
        }
        AMQShortString routingKeyLocal = routingKey == null ? AMQShortString.valueOf("") : routingKey;
        if (isDefaultExchange(exchange)) {
            MessagePublishInfo messagePublishInfo = publishInfoMap.get(routingKeyLocal.toString());
            if (messagePublishInfo != null) {
                setPublishFrame(messagePublishInfo, null);
                return;
            }
            synchronized (this) {
                messagePublishInfo = publishInfoMap.get(routingKeyLocal.toString());
                if (messagePublishInfo != null) {
                    setPublishFrame(messagePublishInfo, null);
                    return;
                }
                ExchangeDeclareParams exchangeParams = new ExchangeDeclareParams();
                exchangeParams.setType(ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
                exchangeParams.setInternal(false);
                exchangeParams.setAutoDelete(false);
                exchangeParams.setDurable(true);
                exchangeParams.setPassive(false);
                getAmqpAdmin().exchangeDeclare(connection.getNamespaceName(),
                        AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE, exchangeParams
                ).thenCompose(__ -> {
                    BindingParams bindingParams = new BindingParams();
                    bindingParams.setRoutingKey(routingKeyLocal.toString());
                    return getAmqpAdmin().queueBind(connection.getNamespaceName(),
                            AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE, routingKeyLocal.toString(), bindingParams);
                }).thenRun(() -> {
                    MessagePublishInfo info =
                            new MessagePublishInfo(
                                    AMQShortString.valueOf(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE),
                                    immediate, mandatory, routingKeyLocal);
                    publishInfoMap.putIfAbsent(routingKeyLocal.toString(), info);
                    setPublishFrame(publishInfoMap.get(routingKeyLocal.toString()), null);
                }).exceptionally(t -> {
                    log.error("Failed to bind queue {} to exchange {}", routingKeyLocal,
                            AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE, t);
                    handleAoPException(t);
                    return null;
                }).join();
            }
        } else {
            MessagePublishInfo info = new MessagePublishInfo(exchange, immediate, mandatory, routingKeyLocal);
            setPublishFrame(info, null);
        }
    }

    private void setPublishFrame(MessagePublishInfo info, final MessageDestination e) {
        currentMessage = new IncomingMessage(info);
        currentMessage.setMessageDestination(e);
    }

    @Override
    protected void deliverCurrentMessageIfComplete() {
        if (currentMessage.allContentReceived()) {
            MessagePublishInfo info = currentMessage.getMessagePublishInfo();
            String exchangeName = AMQShortString.toString(info.getExchange());
            Message<byte[]> message;
            try {
                message = MessageConvertUtils.toPulsarMessage(currentMessage);
            } catch (UnsupportedEncodingException e) {
                connection.sendConnectionClose(INTERNAL_ERROR, "Message encoding fail.", channelId);
                return;
            }

            if (exchangeName == null || exchangeName.length() == 0) {
                exchangeName = AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE;
            }

            Producer<byte[]> producer;
            try {
                producer = getProducer(exchangeName);
            } catch (PulsarServerException e) {
                log.error("Failed to create producer for exchange {}.", exchangeName, e);
                connection.sendConnectionClose(INTERNAL_ERROR,
                        "Failed to create producer for exchange " + exchangeName + ".", channelId);
                return;
            }
            producer.newMessage()
                    .value(message.getData())
                    .properties(message.getProperties())
                    .sendAsync()
                    .thenAccept(position -> {
                        if (log.isDebugEnabled()) {
                            log.debug("Publish message success, position {}", position);
                        }
                        if (confirmOnPublish) {
                            confirmedMessageCounter++;
                            BasicAckBody body = connection.getMethodRegistry().
                                    createBasicAckBody(confirmedMessageCounter, false);
                            connection.writeFrame(body.generateFrame(channelId));
                        }
                    })
                    .exceptionally(throwable -> {
                        log.error("Failed to write message to exchange", throwable);
                        return null;
                    });
        }
    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        // TODO handle message reject, message requeue
        super.messageNAck(deliveryTag, false, requeue);
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        // TODO handle message negative ack, message requeue
        super.messageNAck(deliveryTag, multiple, requeue);
    }

    @Override
    public void close() {
        closeAllConsumers();
        closeAllProducers();
        // TODO need to delete exclusive queues in this channel.
        setDefQueue(null);
    }

    private void closeAllConsumers() {
        if (log.isDebugEnabled()) {
            if (!consumerList.isEmpty()) {
                log.debug("Unsubscribing all consumers on channel  {}", channelId);
            } else {
                log.debug("No consumers to unsubscribe on channel {}", channelId);
            }
        }
        try {
            consumerList.forEach(consumer -> {
                try {
                    consumer.close();
                    // Start expiration detection
                    getAmqpAdmin().startExpirationDetection(connection.getNamespaceName(), consumer.getQueue());
                } catch (Exception e) {
                    log.error("Failed to close consumer.", e);
                }
            });
            consumerList.clear();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private void closeAllProducers() {
        for (Producer<byte[]> producer : producerMap.values()) {
            try {
                producer.close();
            } catch (PulsarClientException e) {
                log.error("Failed to close producer.", e);
            }
        }
    }

    private AmqpAdmin getAmqpAdmin() {
        return this.connection.getAmqpBrokerService().getAmqpAdmin();
    }

    public Producer<byte[]> getProducer(String exchange) throws PulsarServerException {
        PulsarClient client = connection.getPulsarService().getClient();
        return producerMap.computeIfAbsent(exchange, k -> {
            try {
                Producer<byte[]> producer = client.newProducer()
                        .topic(getTopicName(PersistentExchange.TOPIC_PREFIX, exchange))
                        .enableBatching(false)
                        .create();
                getAmqpAdmin().loadExchange(connection.getNamespaceName(), exchange);
                return producer;
            } catch (PulsarClientException e) {
                throw new AoPServiceRuntimeException.ProducerCreationRuntimeException(e);
            }
        });
    }

    public CompletableFuture<AmqpPulsarConsumer> getConsumer(String queue, String consumerTag, boolean autoAck) {
        PulsarClient client;
        try {
            client = connection.getPulsarService().getClient();
        } catch (PulsarServerException e) {
            return FutureUtil.failedFuture(e);
        }
        CompletableFuture<AmqpPulsarConsumer> consumerFuture = new CompletableFuture<>();
        client.newConsumer()
                .topic(getTopicName(PersistentQueue.TOPIC_PREFIX, queue))
                .subscriptionType(SubscriptionType.Shared)
                .property("client_ip", connection.getClientIp())
                .subscriptionName("AMQP_DEFAULT")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName(UUID.randomUUID().toString())
                .receiverQueueSize(getConnection().getAmqpConfig().getAmqpPulsarConsumerQueueSize())
                .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
                .subscribeAsync()
                .thenAccept(consumer -> {
                    AmqpPulsarConsumer amqpPulsarConsumer;
                    try {
                        amqpPulsarConsumer = new AmqpPulsarConsumer(queue, consumerTag, consumer, autoAck,
                                AmqpMultiBundlesChannel.this,
                                AmqpMultiBundlesChannel.this.connection.getPulsarService());
                    } catch (PulsarServerException | PulsarAdminException e) {
                        throw new RuntimeException(e);
                    }
                    consumerFuture.complete(amqpPulsarConsumer);
                    consumerList.add(amqpPulsarConsumer);
                })
                .exceptionally(t -> {
                    consumerFuture.completeExceptionally(t);
                    return null;
                });
        return consumerFuture;
    }

    private String getTopicName(String topicPrefix, String name) {
        return TopicDomain.persistent + "://"
                + connection.getNamespaceName().getTenant() + "/"
                + connection.getNamespaceName().getLocalName() + "/"
                + topicPrefix + name;
    }

    public void setDefQueue(String queue) {
        defQueue = queue;
    }

    public AMQShortString getDefQueue(AMQShortString queue){
        return queue == null || queue.length() == 0 ?
                defQueue != null ? AMQShortString.valueOf(defQueue) : null : queue;
    }
}
