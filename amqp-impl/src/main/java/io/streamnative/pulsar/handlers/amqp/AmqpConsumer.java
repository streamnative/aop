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
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

/**
 * Amqp consumer Used to forward messages.
 */
@Slf4j
public class AmqpConsumer extends Consumer {

    private final AmqpChannel channel;

    private QueueContainer queueContainer;

    private final boolean autoAck;

    private final String consumerTag;

    private final String queueName;
    /**
     * map(exchangeName,treeMap(indexPosition,msgPosition)) .
     */
    private final Map<String, ConcurrentSkipListMap<PositionImpl, PositionImpl>> unAckMessages;
    private static final AtomicIntegerFieldUpdater<AmqpConsumer> MESSAGE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpConsumer.class, "availablePermits");
    private volatile int availablePermits;

    private static final AtomicIntegerFieldUpdater<AmqpConsumer> ADD_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpConsumer.class, "addPermits");
    private volatile int addPermits = 0;

    private final int maxPermits = 1000;

    public AmqpConsumer(QueueContainer queueContainer, Subscription subscription,
        PulsarApi.CommandSubscribe.SubType subType, String topicName, long consumerId,
        int priorityLevel, String consumerName, int maxUnackedMessages, ServerCnx cnx,
        String appId, Map<String, String> metadata, boolean readCompacted,
        PulsarApi.CommandSubscribe.InitialPosition subscriptionInitialPosition,
        PulsarApi.KeySharedMeta keySharedMeta, AmqpChannel channel, String consumerTag, String queueName,
        boolean autoAck) throws BrokerServiceException {
        super(subscription, subType, topicName, consumerId, priorityLevel, consumerName, maxUnackedMessages,
            cnx, appId, metadata, readCompacted, subscriptionInitialPosition, keySharedMeta);
        this.channel = channel;
        this.queueContainer = queueContainer;
        this.autoAck = autoAck;
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.unAckMessages = new ConcurrentHashMap<>();
    }

    public ChannelPromise sendMessages(List<Entry> entries, EntryBatchSizes batchSizes,  int totalMessages,
                                       long totalBytes, RedeliveryTracker redeliveryTracker) {
        return sendMessages(entries, batchSizes, null, totalMessages, totalBytes, 0, redeliveryTracker);
    }

    @Override
    public ChannelPromise sendMessages(List<Entry> entries, EntryBatchSizes batchSizes,
           EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes, long totalChunkedMessages,
           RedeliveryTracker redeliveryTracker) {
        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] List of messages is empty, triggering write future immediately for consumerId {}");
            }

            return null;
        }
        if (!autoAck) {
            channel.getCreditManager().useCreditForMessages(totalMessages, 0);
            if (!channel.getCreditManager().hasCredit()) {
                channel.setBlockedOnCredit();
            }
        }
        final AmqpConnection connection = channel.getConnection();
        MESSAGE_PERMITS_UPDATER.addAndGet(this, -totalMessages);
        connection.ctx.channel().eventLoop().execute(() -> {
            for (int i = 0; i < entries.size(); i++) {
                Entry index = entries.get(i);
                if (index == null) {
                    // Entry was filtered out
                    continue;
                }
                IndexMessage indexMessage = MessageConvertUtils.entryToIndexMessage(index);
                if (indexMessage == null) {
                    continue;
                }
                CompletableFuture<Entry> entryCompletableFuture = getQueue().readEntryAsync(
                    indexMessage.getExchangeName(), indexMessage.getLedgerId(), indexMessage.getEntryId());
                entryCompletableFuture.whenComplete((msg, ex) -> {
                    if (ex == null) {
                        long deliveryTag = channel.getNextDeliveryTag();

                        addUnAckMessages(indexMessage.getExchangeName(), (PositionImpl) index.getPosition(),
                                (PositionImpl) msg.getPosition());
                        if (!autoAck) {
                            channel.getUnacknowledgedMessageMap().add(deliveryTag,
                                    index.getPosition(), this, msg.getLength());
                        }

                        try {
                            connection.getAmqpOutputConverter().writeDeliver(MessageConvertUtils.entryToAmqpBody(msg),
                                channel.getChannelId(), getRedeliveryTracker().contains(index.getPosition()),
                                deliveryTag, AMQShortString.createAMQShortString(consumerTag));
                        } catch (UnsupportedEncodingException e) {
                            log.error("sendMessages UnsupportedEncodingException", e.getMessage());
                        }

                        if (autoAck) {
                            messagesAck(index.getPosition());
                        }
                    } else {
                        messagesAck(index.getPosition());
                    }
                    msg.release();
                    index.release();
                    indexMessage.recycle();
                });
            }
            batchSizes.recyle();
        });
        return null;
    }

    public void messagesAck(List<Position> position) {
        incrementPermits(position.size());
        ManagedCursor cursor = ((PersistentSubscription) getSubscription()).getCursor();
        Position previousMarkDeletePosition = cursor.getMarkDeletedPosition();
        getSubscription().acknowledgeMessage(position, PulsarApi.CommandAck.AckType.Individual, Collections.EMPTY_MAP);
        if (!cursor.getMarkDeletedPosition().equals(previousMarkDeletePosition)) {
            synchronized (this) {
                PositionImpl newDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
                unAckMessages.forEach((key, value) -> {
                    SortedMap<PositionImpl, PositionImpl> ackMap = value.headMap(newDeletePosition, true);
                    if (ackMap.size() > 0) {
                        PositionImpl lastValue = ackMap.get(ackMap.lastKey());
                        getQueue().acknowledgeAsync(key, lastValue.getLedgerId(), lastValue.getEntryId());
                    }
                    ackMap.clear();
                });
            }
        }
    }

    public void messagesAck(Position position) {
        messagesAck(Collections.singletonList(position));
    }

    public void redeliverAmqpMessages(List<PositionImpl> positions) {
        getSubscription().getDispatcher().redeliverUnacknowledgedMessages(this, positions);
    }

    public RedeliveryTracker getRedeliveryTracker() {
        return getSubscription().getDispatcher().getRedeliveryTracker();
    }

    public AmqpQueue getQueue() {
        return queueContainer.getQueue(channel.getConnection().getNamespaceName(), queueName);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public int getAvailablePermits() {
        if (autoAck || channel.getCreditManager().isNoCreditLimit()) {
            return availablePermits;
        }
        return this.channel.getCreditManager().hasCredit()
            ? (int) this.channel.getCreditManager().getMessageCredit() : 0;
    }

    void addUnAckMessages(String exchangeName, PositionImpl index, PositionImpl message) {
        ConcurrentSkipListMap<PositionImpl, PositionImpl> map = unAckMessages.computeIfAbsent(exchangeName,
                treeMap -> new ConcurrentSkipListMap<>());
        map.put(index, message);
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public void handleFlow(int permits) {
        MESSAGE_PERMITS_UPDATER.getAndAdd(this, permits);
        getSubscription().getDispatcher().consumerFlow(this, permits);
    }

    public void incrementPermits(int permits) {
        int var = ADD_PERMITS_UPDATER.addAndGet(this, permits);
        if (var > maxPermits / 2) {
            MESSAGE_PERMITS_UPDATER.addAndGet(this, var);
            this.getSubscription().consumerFlow(this, availablePermits);
            ADD_PERMITS_UPDATER.set(this, 0);
        }
    }
}
