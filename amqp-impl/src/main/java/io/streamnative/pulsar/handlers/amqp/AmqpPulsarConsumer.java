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

import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import io.streamnative.pulsar.handlers.amqp.utils.TopicUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.BackoffBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

/**
 * AMQP Pulsar consumer.
 */
@Slf4j
public class AmqpPulsarConsumer implements UnacknowledgedMessageMap.MessageProcessor {

    private final String consumerTag;
    private final Consumer<byte[]> consumer;
    private final AmqpChannel amqpChannel;
    private final ScheduledExecutorService executorService;
    private final boolean autoAck;
    private volatile boolean isClosed = false;
    private final Backoff consumeBackoff;
    private CompletableFuture<Producer<byte[]>> producer;
    private final PulsarAdmin pulsarAdmin;
    private String routingKey;
    private String dleExchangeName;
    @Getter
    private final String queue;

    public AmqpPulsarConsumer(String queue, String consumerTag, Consumer<byte[]> consumer, boolean autoAck, AmqpChannel amqpChannel,
                              PulsarService pulsarService)
            throws PulsarAdminException, PulsarServerException {
        this.queue = queue;
        this.consumerTag = consumerTag;
        this.consumer = consumer;
        this.autoAck = autoAck;
        this.amqpChannel = amqpChannel;
        this.executorService = pulsarService.getExecutor();
        this.pulsarAdmin = pulsarService.getAdminClient();
        Map<String, String> properties = pulsarAdmin.topics().getProperties(consumer.getTopic());
        String args = properties.get(PersistentQueue.ARGUMENTS);
        if (StringUtils.isNotBlank(args)) {
            Map<String, Object> arguments = QueueUtil.covertStringValueAsObjectMap(args);
            Object dleExchangeName;
            String dleName;
            this.routingKey = (String) arguments.get("x-dead-letter-routing-key");
            if ((dleExchangeName = arguments.get(PersistentQueue.X_DEAD_LETTER_EXCHANGE)) != null
                    && StringUtils.isNotBlank(dleName = dleExchangeName.toString())
                    && StringUtils.isNotBlank(routingKey)) {
                NamespaceName namespaceName = TopicName.get(consumer.getTopic()).getNamespaceObject();
                String topic = TopicUtil.getTopicName(PersistentExchange.TOPIC_PREFIX,
                        namespaceName.getTenant(), namespaceName.getLocalName(), dleName);
                this.dleExchangeName = dleExchangeName.toString();
                this.producer = pulsarService.getClient().newProducer()
                        .topic(topic)
                        .enableBatching(false)
                        .createAsync();
            }
        }
        this.consumeBackoff = new BackoffBuilder()
                .setInitialTime(1, TimeUnit.MILLISECONDS)
                .setMax(1, TimeUnit.SECONDS)
                .setMandatoryStop(0, TimeUnit.SECONDS)
                .create();
    }

    public void startConsume() {
        executorService.submit(this::consume);
    }

    private void consume() {
        if (isClosed) {
            return;
        }

        Message<byte[]> message;
        try {
            message = this.consumer.receive(0, TimeUnit.SECONDS);
            if (message == null) {
                this.executorService.schedule(this::consume, consumeBackoff.next(), TimeUnit.MILLISECONDS);
                return;
            }

            MessageIdImpl messageId = (MessageIdImpl) message.getMessageId();
            long deliveryIndex = this.amqpChannel.getNextDeliveryTag();
            this.amqpChannel.getConnection().getAmqpOutputConverter().writeDeliver(
                    MessageConvertUtils.messageToAmqpBody(message),
                    this.amqpChannel.getChannelId(),
                    false,
                    deliveryIndex,
                    AMQShortString.createAMQShortString(this.consumerTag));
            if (this.autoAck) {
                this.consumer.acknowledgeAsync(messageId).exceptionally(t -> {
                    log.error("Failed to ack message {} for topic {} by auto ack.",
                            messageId, consumer.getTopic(), t);
                    return null;
                });
            } else {
                this.amqpChannel.getUnacknowledgedMessageMap().add(
                        deliveryIndex, PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId()),
                        AmqpPulsarConsumer.this, message.size());
            }
            consumeBackoff.reset();
            this.consume();
        } catch (Exception e) {
            long backoff = consumeBackoff.next();
            log.error("Failed to receive message and send to client, retry in {} ms.", backoff, e);
            this.executorService.schedule(this::consume, backoff, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void messageAck(Position position) {
        consumer.acknowledgeAsync(new MessageIdImpl(position.getLedgerId(), position.getEntryId(), -1));
    }

    @Override
    public void requeue(List<PositionImpl> positions) {
        for (PositionImpl pos : positions) {
            consumer.negativeAcknowledge(new MessageIdImpl(pos.getLedgerId(), pos.getEntryId(), -1));
        }
    }

    @Override
    public void discardMessage(List<PositionImpl> positions) {
        if (producer == null || routingKey == null) {
            for (PositionImpl pos : positions) {
                consumer.acknowledgeAsync(new MessageIdImpl(pos.getLedgerId(), pos.getEntryId(), -1));
            }
            return;
        }
        for (PositionImpl pos : positions) {
            pulsarAdmin.topics().getMessageByIdAsync(consumer.getTopic(), pos.getLedgerId(), pos.getEntryId())
                    .thenAccept(message -> {
                        Map<String, String> properties = new HashMap<>(message.getProperties());
                        properties.put(MessageConvertUtils.PROP_ROUTING_KEY, routingKey);
                        properties.put(MessageConvertUtils.PROP_EXCHANGE, dleExchangeName);
                        properties.put(MessageConvertUtils.PROP_EXPIRATION, "0");
                        TypedMessageBuilderImpl<byte[]> messageBuilder =
                                new TypedMessageBuilderImpl<>(null, Schema.BYTES);
                        messageBuilder.properties(properties);
                        messageBuilder.value(message.getValue());
                        producer.thenAccept(p -> {
                            if (p instanceof ProducerImpl<byte[]> producerImpl) {
                                producerImpl.sendAsync(messageBuilder.getMessage())
                                        .thenCompose(messageId -> consumer.acknowledgeAsync(
                                                new MessageIdImpl(pos.getLedgerId(), pos.getEntryId(), -1)));
                            }
                        });
                    }).exceptionally(throwable -> {
                        log.error("Query message [{}] fail", pos, throwable);
                        return null;
                    });
        }
    }

    public void close() throws PulsarClientException {
        this.isClosed = true;
        this.consumer.pause();
        this.consumer.close();
    }

}
