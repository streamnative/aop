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

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;


/**
 * Amqp consumer Used to forward messages.
 */
@Slf4j
public class AmqpConsumerOriginal extends AmqpConsumer {

    public AmqpConsumerOriginal(QueueContainer queueContainer, Subscription subscription,
                                CommandSubscribe.SubType subType, String topicName, long consumerId,
                                int priorityLevel, String consumerName, boolean isDurable, ServerCnx cnx,
                                String appId, Map<String, String> metadata, boolean readCompacted,
                                MessageId messageId,
                                KeySharedMeta keySharedMeta, AmqpChannel channel, String consumerTag, String queueName,
                                boolean autoAck) throws BrokerServiceException {
        super(queueContainer, subscription, subType, topicName, consumerId, priorityLevel, consumerName, isDurable,
                cnx, appId, metadata, readCompacted, messageId, keySharedMeta, channel, consumerTag,
                queueName, autoAck);
    }

    @Override
    public Future<Void> sendMessages(final List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes,
                                     long totalChunkedMessages, RedeliveryTracker redeliveryTracker) {
        return sendMessages(entries, batchSizes, batchIndexesAcks, totalMessages, totalBytes,
                totalChunkedMessages, redeliveryTracker, Commands.DEFAULT_CONSUMER_EPOCH);
    }

    @Override
    public Future<Void> sendMessages(final List<? extends Entry> entries, EntryBatchSizes batchSizes,
                                     EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes,
                                     long totalChunkedMessages, RedeliveryTracker redeliveryTracker, long epoch) {
        ChannelPromise writePromise = this.channel.getConnection().getCtx().newPromise();
        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}]({}) List of messages is empty, trigger write future immediately for consumerId {}",
                        queueName, consumerTag, consumerId());
            }
            writePromise.setSuccess(null);
            return writePromise;
        }
        if (!autoAck) {
            channel.getCreditManager().useCreditForMessages(totalMessages, 0);
            if (!channel.getCreditManager().hasCredit()) {
                channel.setBlockedOnCredit();
            }
        }
        MESSAGE_PERMITS_UPDATER.addAndGet(this, -totalMessages);
        final AmqpConnection connection = channel.getConnection();
        connection.ctx.channel().eventLoop().execute(() -> {
            double count = 0;
            for (Entry entry : entries) {
                if (entry == null) {
                    // Entry was filtered out
                    continue;
                }
                count++;
                sendMessage(entry);
            }
            connection.getCtx().writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            batchSizes.recyle();
        });
        return writePromise;
    }

    private void sendMessage(Entry entry) {
        try {
            long deliveryTag = channel.getNextDeliveryTag();
            if (!autoAck) {
                channel.getUnacknowledgedMessageMap().add(deliveryTag,
                        entry.getPosition(), this, entry.getLength());
            }

            channel.getConnection().getAmqpOutputConverter().writeDeliver(
                    MessageConvertUtils.entryToAmqpBody(entry),
                    channel.getChannelId(),
                    getRedeliveryTracker().contains(entry.getPosition()),
                    deliveryTag,
                    AMQShortString.createAMQShortString(consumerTag));

            if (autoAck) {
                messagesAck(entry.getPosition());
            }
        } catch (Exception e) {
            log.error("[{}]({}) Failed to send message to consumer.", queueName, consumerTag, e);
        } finally {
            entry.release();
        }
    }

}
