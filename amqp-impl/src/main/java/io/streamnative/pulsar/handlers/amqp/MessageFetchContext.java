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

import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetEmptyBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;

/**
 * MessageFetchContext handling BasicGetRequest.
 */
@Slf4j
public final class MessageFetchContext {

    private final Handle<MessageFetchContext> recyclerHandle;

    private MessageFetchContext(Handle<MessageFetchContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<MessageFetchContext> RECYCLER = new Recycler<MessageFetchContext>() {

        @Override
        protected MessageFetchContext newObject(Handle<MessageFetchContext> handle) {
            return new MessageFetchContext(handle);
        }
    };

    private void recycle() {
        recyclerHandle.recycle(this);
    }

    // handle request
    public static void handleFetch(AmqpChannel channel, AmqpConsumer consumer, boolean autoAck) {
        MessageFetchContext context = RECYCLER.get();
        if (!autoAck && !channel.getCreditManager().hasCredit()) {
            MethodRegistry methodRegistry = channel.getConnection().getMethodRegistry();
            BasicGetEmptyBody responseBody = methodRegistry.createBasicGetEmptyBody(null);
            channel.getConnection().writeFrame(responseBody.generateFrame(channel.getChannelId()));
            return;
        }
        ManagedCursor cursor = ((PersistentSubscription) consumer.getSubscription()).getCursor();
        CompletableFuture<Pair<Position, AmqpMessageData>> message = new CompletableFuture<>();
        message.whenComplete((pair, throwable) -> {
            if (pair != null && pair.getRight() != null) {
                long deliveryTag = channel.getNextDeliveryTag();
                channel.getConnection().getAmqpOutputConverter().writeGetOk(pair.getRight(), channel.getChannelId(),
                    consumer.getRedeliveryTracker().contains(pair.getLeft()), deliveryTag, 0);
                if (autoAck) {
                    consumer.messagesAck(pair.getLeft());
                } else {
                    channel.getUnacknowledgedMessageMap().add(deliveryTag, pair.getLeft(), consumer, 0);
                    channel.getCreditManager().useCreditForMessages(1, 0);
                }
            } else {
                if (pair != null && pair.getLeft() != null) {
                    consumer.messagesAck(pair.getLeft());
                }
                MethodRegistry methodRegistry = channel.getConnection().getMethodRegistry();
                BasicGetEmptyBody responseBody = methodRegistry.createBasicGetEmptyBody(null);
                channel.getConnection().writeFrame(responseBody.generateFrame(channel.getChannelId()));
            }
        });

        cursor.asyncReadEntries(1, new ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> list, Object o) {

                if (list.size() <= 0) {
                    message.complete(null);
                    return;
                }
                Entry index = list.get(0);
                IndexMessage indexMessage = MessageConvertUtils.entryToIndexMessage(index);
//                if (indexMessage == null) {
//                    message.complete(Pair.of(index.getPosition(), null));
//                    return;
//                }
                consumer.asyncGetQueue().thenApply(amqpQueue -> amqpQueue.readEntryAsync(
                        indexMessage.getExchangeName(), indexMessage.getLedgerId(), indexMessage.getEntryId())
                        .whenComplete((msg, ex) -> {
                            if (ex == null) {
                                try {
                                    message.complete(Pair.of(index.getPosition(),
                                            MessageConvertUtils.entryToAmqpBody(msg)));
                                } catch (Exception e) {
                                    log.error("Failed to convert entry to AMQP body", e);
                                }
                                consumer.addUnAckMessages(indexMessage.getExchangeName(),
                                        (PositionImpl) index.getPosition(), (PositionImpl) msg.getPosition());
                            } else {
                                message.complete(Pair.of(index.getPosition(), null));
                            }
                            msg.release();
                            index.release();
                            indexMessage.recycle();
                            context.recycle();
                        })
                ).exceptionally(throwable -> {
                    log.error("Failed to get queue from queue container", throwable);
                    return null;
                });
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException e, Object o) {
                message.complete(null);
            }
        }, null, null);

    }

}
