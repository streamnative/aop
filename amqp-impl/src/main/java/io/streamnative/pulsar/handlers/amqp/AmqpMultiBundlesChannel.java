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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;

/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpMultiBundlesChannel extends AmqpChannel {

    private final Map<String, Producer<byte[]>> producerMap;
    private final List<AmqpPulsarConsumer> consumerList;

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
                connection.getNamespaceName().toString(), exchange.toString(), params).thenAccept(__ -> {
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
        params.setArguments(FieldTable.convertToMap(arguments));

        getAmqpAdmin().queueDeclare(
                connection.getNamespaceName().toString(), queue.toString(), params).thenAccept(amqpQueue -> {
//            setDefaultQueue(amqpQueue);
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
        params.setRoutingKey(bindingKey != null ? bindingKey.toString() : null);
        params.setArguments(FieldTable.convertToMap(argumentsTable));

        getAmqpAdmin().queueBind(connection.getNamespaceName().toString(),
                exchange.toString(), queue.toString(), params).thenAccept(__ -> {
            MethodRegistry methodRegistry = connection.getMethodRegistry();
            AMQMethodBody responseBody = methodRegistry.createQueueBindOkBody();
            connection.writeFrame(responseBody.generateFrame(channelId));
        }).exceptionally(t -> {
            log.error("Failed to bind queue {} to exchange {}.", queue, exchange, t);
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

        getAmqpAdmin().queueUnbind(connection.getNamespaceName().toString(), exchange.toString(),
                queue.toString(), bindingKey.toString()).thenAccept(__ -> {
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

    public void close() {
        closeAllConsumers();
        closeAllProducers();
        // TODO need to delete exclusive queues in this channel.
        setDefaultQueue(null);
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
                return client.newProducer()
                        .topic(getTopicName(PersistentExchange.TOPIC_PREFIX, exchange))
                        .enableBatching(false)
                        .create();
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
                .subscriptionName("AMQP_DEFAULT")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .consumerName(UUID.randomUUID().toString())
                .receiverQueueSize(getConnection().getAmqpConfig().getAmqpPulsarConsumerQueueSize())
                .subscribeAsync()
                .thenAccept(consumer-> {
                    AmqpPulsarConsumer amqpPulsarConsumer = new AmqpPulsarConsumer(consumerTag, consumer, autoAck,
                            AmqpMultiBundlesChannel.this,
                            AmqpMultiBundlesChannel.this.connection.getPulsarService().getExecutor());
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
                + connection.getAmqpConfig().getAmqpTenant() + "/"
                + connection.getNamespaceName().getLocalName() + "/"
                + topicPrefix + name;
    }

}
