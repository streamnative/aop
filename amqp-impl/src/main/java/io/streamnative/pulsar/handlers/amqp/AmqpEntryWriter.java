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

import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.messageToByteBuf;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Message;

/**
 * This class used to write entry to ledger.
 */
@Slf4j
public class AmqpEntryWriter implements AsyncCallbacks.AddEntryCallback {

    private final PersistentTopic topic;

    public AmqpEntryWriter(PersistentTopic persistentTopic) {
        this.topic = persistentTopic;
    }

    public CompletableFuture<Position> publishMessage(Message<byte[]> message) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        ByteBuf data = messageToByteBuf(message);
        try {
            if (topic.getBrokerService().isBrokerEntryMetadataEnabled()) {
                topic.getManagedLedger().asyncAddEntry(data, 1, this,
                        MessagePublishContext.get(System.nanoTime(), future));
            } else {
                topic.getManagedLedger().asyncAddEntry(data, this,
                        MessagePublishContext.get(System.nanoTime(), future));
            }
        } finally {
            data.release();
        }
        return future;
    }

    protected static class MessagePublishContext {
        private long startTimeNs;
        private CompletableFuture<Position> positionFuture;

        private final Recycler.Handle<MessagePublishContext> handle;

        private MessagePublishContext(Recycler.Handle<MessagePublishContext> handle) {
            this.handle = handle;
        }

        public static MessagePublishContext get(long startTimeNs, CompletableFuture<Position> positionFuture) {
            MessagePublishContext context = RECYCLER.get();
            context.startTimeNs = startTimeNs;
            context.positionFuture = positionFuture;
            return context;
        }

        private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
            @Override
            protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
                return new MessagePublishContext(handle);
            }
        };

        private void recycle() {
            this.startTimeNs = -1;
            if (this.positionFuture != null && !this.positionFuture.isDone()) {
                this.positionFuture.cancel(true);
            }
            this.positionFuture = null;
        }

    }

    @Override
    public void addComplete(Position position, ByteBuf entryData, Object ctx) {
        MessagePublishContext context = (MessagePublishContext) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Success to write entry with position {}.", topic.getName(), position);
        }
        topic.recordAddLatency(System.nanoTime() - context.startTimeNs, TimeUnit.NANOSECONDS);
        topic.getTransactionBuffer().syncMaxReadPositionForNormalPublish(position, false);
        context.positionFuture.complete(position);
        context.recycle();
    }

    @Override
    public void addFailed(ManagedLedgerException exception, Object ctx) {
        MessagePublishContext context = (MessagePublishContext) ctx;
        log.error("[{}] Failed to write entry.", topic.getName(), exception);
        context.positionFuture.completeExceptionally(exception);
        context.recycle();
    }

}
