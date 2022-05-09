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
        if (topic.getBrokerService().isBrokerEntryMetadataEnabled()) {
            topic.getManagedLedger().asyncAddEntry(
                    messageToByteBuf(message), 1, this,
                    MessagePublishContext.get(System.nanoTime(), future));
        } else {
            topic.getManagedLedger().asyncAddEntry(
                    messageToByteBuf(message), this,
                    MessagePublishContext.get(System.nanoTime(), future));
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
        topic.recordAddLatency(System.nanoTime() - context.startTimeNs, TimeUnit.MICROSECONDS);
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
