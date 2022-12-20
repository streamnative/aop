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
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageIdImpl;
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

    public AmqpPulsarConsumer(String consumerTag, Consumer<byte[]> consumer, boolean autoAck, AmqpChannel amqpChannel,
                              ScheduledExecutorService executorService) {
        this.consumerTag = consumerTag;
        this.consumer = consumer;
        this.autoAck = autoAck;
        this.amqpChannel = amqpChannel;
        this.executorService = executorService;
    }

    public void startConsume() {
        executorService.submit(this::consume);
    }

    private void consume() {
        Message<byte[]> message;
        try {
            message = this.consumer.receive(0, TimeUnit.SECONDS);
            if (message == null) {
                this.executorService.schedule(this::consume, 100, TimeUnit.MILLISECONDS);
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
            this.consume();
        } catch (Exception e) {
            log.error("Failed to send message", e);
            throw new RuntimeException(e);
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

}
