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

import io.netty.channel.ChannelPromise;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

/**
 * Amqp consumer Used to forward messages.
 */
@Slf4j
public class AmqpConsumer extends Consumer {

    private final AmqpChannel channel;

    private final boolean autoAck;

    private final String consumerTag;

    public AmqpConsumer(Subscription subscription,
        PulsarApi.CommandSubscribe.SubType subType, String topicName, long consumerId,
        int priorityLevel, String consumerName, int maxUnackedMessages, ServerCnx cnx,
        String appId, Map<String, String> metadata, boolean readCompacted,
        PulsarApi.CommandSubscribe.InitialPosition subscriptionInitialPosition,
        PulsarApi.KeySharedMeta keySharedMeta, AmqpChannel channel, String consumerTag,
        boolean autoAck) throws BrokerServiceException {
        super(subscription, subType, topicName, consumerId, priorityLevel, consumerName, maxUnackedMessages,
            cnx, appId, metadata, readCompacted, subscriptionInitialPosition, keySharedMeta);
        this.channel = channel;
        this.autoAck = autoAck;
        this.consumerTag = consumerTag;
    }

    @Override public ChannelPromise sendMessages(List<Entry> entries, EntryBatchSizes batchSizes, int totalMessages,
        long totalBytes, RedeliveryTracker redeliveryTracker) {
        final AmqpConnection connection = channel.getConnection();
        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] List of messages is empty, triggering write future immediately for consumerId {}");
            }

            return null;
        }

        connection.ctx.channel().eventLoop().execute(() -> {
            List<Position> autoAcks = new ArrayList<>();
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (entry == null) {
                    // Entry was filtered out
                    continue;
                }
                long deliveryTag = channel.getNextDeliveryTag();
                connection.getAmqpOutputConverter().writeDeliver(MessageConvertUtils.entriesToAmqpBody(entry),
                    channel.getChannelId(), false, channel.getNextDeliveryTag(),
                    AMQShortString.createAMQShortString(consumerTag));
                if (autoAck) {
                    // TODO confirm
                    autoAcks.add(entry.getPosition());
                } else {
                    channel.getUnacknowledgedMessageMap().add(deliveryTag, entry.getPosition(), this);
                }
                entry.release();
            }
            if (autoAck) {
                messagesAck(autoAcks, PulsarApi.CommandAck.AckType.Individual, null);
            }
            batchSizes.recyle();
        });

        return null;
    }

    public void messagesAck(List<Position> positions, PulsarApi.CommandAck.AckType ackType,
        Map<String, Long> properties) {
        getSubscription().acknowledgeMessage(positions, ackType, properties);
    }

    public void redeliverAMQMessages(List<PositionImpl> positions) {
        getSubscription().redeliverUnacknowledgedMessages(this, positions);
    }

    @Override public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override public int hashCode() {
        return super.hashCode();
    }
}
