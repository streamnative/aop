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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

/**
 * Amqp consumer Used to forward messages.
 */
@Slf4j
public class AmqpConsumer extends Consumer implements UnacknowledgedMessageMap.MessageProcessor {

    protected final AmqpChannel channel;

    private QueueContainer queueContainer;

    protected final boolean autoAck;

    protected final String consumerTag;

    protected final String queueName;
    /**
     * map(exchangeName,treeMap(indexPosition,msgPosition)) .
     */
    private final Map<String, ConcurrentSkipListMap<PositionImpl, PositionImpl>> unAckMessages;
    protected static final AtomicIntegerFieldUpdater<AmqpConsumer> MESSAGE_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpConsumer.class, "availablePermits");
    private volatile int availablePermits;

    private static final AtomicIntegerFieldUpdater<AmqpConsumer> ADD_PERMITS_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpConsumer.class, "addPermits");
    private volatile int addPermits = 0;

    private final int maxPermits = 1000;

    protected Field msgOutField;
    protected Field msgOutCounterField;
    protected Field bytesOutCounterField;
    protected Method recordMultpleEventsMethod;
    protected Method addMethod;

    public AmqpConsumer(QueueContainer queueContainer, Subscription subscription,
        CommandSubscribe.SubType subType, String topicName, long consumerId,
        int priorityLevel, String consumerName, boolean isDurable, ServerCnx cnx,
        String appId, Map<String, String> metadata, boolean readCompacted, MessageId messageId,
        KeySharedMeta keySharedMeta, AmqpChannel channel, String consumerTag, String queueName,
        boolean autoAck) {
        super(subscription, subType, topicName, consumerId, priorityLevel, consumerName, isDurable,
            cnx, appId, metadata, readCompacted, keySharedMeta, messageId, Commands.DEFAULT_CONSUMER_EPOCH);
        this.channel = channel;
        this.queueContainer = queueContainer;
        this.autoAck = autoAck;
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.unAckMessages = new ConcurrentHashMap<>();

        try {
            this.msgOutField = Consumer.class.getDeclaredField("msgOut");
            this.msgOutField.setAccessible(true);
            this.msgOutCounterField = Consumer.class.getDeclaredField("msgOutCounter");
            this.msgOutCounterField.setAccessible(true);
            this.bytesOutCounterField = Consumer.class.getDeclaredField("bytesOutCounter");
            this.bytesOutCounterField.setAccessible(true);
            this.recordMultpleEventsMethod =
                    Rate.class.getDeclaredMethod("recordMultipleEvents", long.class, long.class);
            this.addMethod = LongAdder.class.getDeclaredMethod("add", long.class);
        } catch (Exception e) {
            log.warn("Failed to get stats field.", e);
        }
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
           EntryBatchIndexesAcks batchIndexesAcks, int totalMessages, long totalBytes, long totalChunkedMessages,
           RedeliveryTracker redeliveryTracker, long epoch) {
        ChannelPromise writePromise = this.channel.getConnection().getCtx().newPromise();
        if (entries.isEmpty() || totalMessages == 0) {
            if (log.isDebugEnabled()) {
                log.debug("[{}-{}] List of messages is empty, triggering write future immediately for consumerId {}",
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
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (Entry index : entries) {
                if (index == null) {
                    // Entry was filtered out
                    continue;
                }
                futures.add(sendMessage(index));
            }
            FutureUtil.waitForAll(futures).whenComplete((ignored, throwable) -> {
                if (throwable != null) {
                    writePromise.setFailure(throwable);
                    return;
                }
                try {
                    this.recordMultpleEventsMethod.invoke(this.msgOutField.get(this), totalMessages, totalBytes);
                    this.addMethod.invoke(this.msgOutCounterField.get(this), totalMessages);
                    this.addMethod.invoke(this.bytesOutCounterField.get(this), totalBytes);
                } catch (Exception e) {
                    log.error("Failed to record delivery stats.", e);
                }
//                this.msgOut.recordMultipleEvents((long)totalMessages, totalBytes);
//                this.msgOutCounter.add((long)totalMessages);
//                this.bytesOutCounter.add(totalBytes);
                connection.getCtx().writeAndFlush(Unpooled.EMPTY_BUFFER, writePromise);
            });
            batchSizes.recyle();
        });
        return writePromise;
    }

    private CompletableFuture<Void> sendMessage(Entry index) {
        CompletableFuture<Void> sendFuture = new CompletableFuture<>();
        IndexMessage indexMessage;
        try {
            indexMessage = MessageConvertUtils.entryToIndexMessage(index);
        } catch (Exception e) {
            log.error("[{}-{}] Failed to get index data.", queueName, consumerTag, e);
            sendFuture.completeExceptionally(e);
            return sendFuture;
        }
        asyncGetQueue()
                .thenCompose(amqpQueue -> amqpQueue.readEntryAsync(
                        indexMessage.getExchangeName(), indexMessage.getLedgerId(), indexMessage.getEntryId())
                .thenAccept(msg -> {
                    try {
                        long deliveryTag = channel.getNextDeliveryTag();

                        addUnAckMessages(indexMessage.getExchangeName(), (PositionImpl) index.getPosition(),
                                (PositionImpl) msg.getPosition());
                        if (!autoAck) {
                            channel.getUnacknowledgedMessageMap().add(deliveryTag,
                                    index.getPosition(), this, msg.getLength());
                        }

                        try {
<<<<<<< HEAD
                            boolean isRedelivery = getRedeliveryTracker().getRedeliveryCount(
                                    index.getPosition().getLedgerId(), index.getPosition().getEntryId()) > 0;
=======
                            boolean isRedelivery = getRedeliveryTracker()
                                    .getRedeliveryCount(index.getPosition().getLedgerId(),
                                            index.getPosition().getEntryId()) > 0;
>>>>>>> 939932c (fix)
                            channel.getConnection().getAmqpOutputConverter().writeDeliver(
                                    MessageConvertUtils.entryToAmqpBody(msg),
                                    channel.getChannelId(),
                                    isRedelivery,
                                    deliveryTag,
                                    AMQShortString.createAMQShortString(consumerTag));
                            sendFuture.complete(null);
                        } catch (Exception e) {
                            log.error("[{}-{}] Failed to send message to consumer.", queueName, consumerTag, e);
                            sendFuture.completeExceptionally(e);
                            return;
                        } finally {
                            msg.release();
                        }

                        if (autoAck) {
                            messageAck(index.getPosition());
                        }
                    } finally {
                        index.release();
                        indexMessage.recycle();
                    }
                })).exceptionally(throwable -> {
                    log.error("[{}-{}] Failed to read data from exchange topic {}.",
                            queueName, consumerTag, indexMessage.getExchangeName(), throwable);
                    sendFuture.completeExceptionally(throwable);
                    return null;
        });
        return sendFuture;
    }

    public void ack(List<Position> position) {
        incrementPermits(position.size());
        ManagedCursor cursor = ((PersistentSubscription) getSubscription()).getCursor();
        Position previousMarkDeletePosition = cursor.getMarkDeletedPosition();
        getSubscription().acknowledgeMessage(position, CommandAck.AckType.Individual, Collections.EMPTY_MAP);
        if (!cursor.getMarkDeletedPosition().equals(previousMarkDeletePosition)) {
            asyncGetQueue().whenComplete((amqpQueue, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to get queue from queue container", throwable);
                } else {
                    synchronized (this) {
                        PositionImpl newDeletePosition = (PositionImpl) cursor.getMarkDeletedPosition();
                        unAckMessages.forEach((key, value) -> {
                            SortedMap<PositionImpl, PositionImpl> ackMap = value.headMap(newDeletePosition, true);
                            if (ackMap.size() > 0) {
                                PositionImpl lastValue = ackMap.get(ackMap.lastKey());
                                amqpQueue.acknowledgeAsync(key, lastValue.getLedgerId(), lastValue.getEntryId());
                            }
                            ackMap.clear();
                        });
                    }
                }
            });
        }
    }

    @Override
    public void messageAck(Position position) {
        ack(Collections.singletonList(position));
    }

    @Override
    public void requeue(List<PositionImpl> positions) {
        getSubscription().getDispatcher().redeliverUnacknowledgedMessages(this, positions);
    }

    public RedeliveryTracker getRedeliveryTracker() {
        return getSubscription().getDispatcher().getRedeliveryTracker();
    }

    public CompletableFuture<AmqpQueue> asyncGetQueue() {
        return queueContainer.asyncGetQueue(channel.getConnection().getNamespaceName(), queueName, false);
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

    @Override
    public boolean isWritable() {
        return channel.getConnection().ctx.channel().isWritable();
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

    public synchronized void incrementPermits(int permits) {
        int var = ADD_PERMITS_UPDATER.addAndGet(this, permits);
        if (var > maxPermits / 2) {
            MESSAGE_PERMITS_UPDATER.addAndGet(this, var);
            this.getSubscription().consumerFlow(this, var);
            ADD_PERMITS_UPDATER.set(this, 0);
        }
    }

}
