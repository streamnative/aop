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

import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

import io.netty.util.Recycler;
import io.streamnative.pulsar.handlers.amqp.impl.DirectMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.FanoutMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryExchange;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryQueue;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.impl.TopicMessageRouter;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Base class for AMQP message router.
 */
@Slf4j
public abstract class AbstractAmqpMessageRouter implements AmqpMessageRouter, AsyncCallbacks.ReadEntriesCallback,
        AsyncCallbacks.DeleteCallback {

    protected AmqpExchange exchange;
    protected AmqpQueue queue;
    protected final AmqpMessageRouter.Type routerType;
    protected Set<String> bindingKeys;
    protected Map<String, Object> arguments;
    private String routerSummary;

    private ScheduledExecutorService scheduledExecutorService;
    protected ManagedCursor cursor;
    private int defaultReadBatchSize = 100;
    private int defaultReadMaxSizeBytes = 4 * 1024 * 1024;

    protected final Backoff startBackOff = new Backoff(
            100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private static final AtomicReferenceFieldUpdater<AbstractAmqpMessageRouter, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AbstractAmqpMessageRouter.class, State.class, "state");
    private volatile State state  = State.Stopped;

    /**
     * Router state.
     */
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final int routerQueueSize = 1000;
    private volatile int pendingQueueSize = 0;
    private static final AtomicIntegerFieldUpdater<AbstractAmqpMessageRouter> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractAmqpMessageRouter.class, "pendingQueueSize");

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<AbstractAmqpMessageRouter> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractAmqpMessageRouter.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Backoff readFailureBackoff = new Backoff(
            1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    protected AbstractAmqpMessageRouter(Type routerType, ScheduledExecutorService scheduledExecutorService) {
        this.routerType = routerType;
        this.bindingKeys = new HashSet<>();
        this.scheduledExecutorService = scheduledExecutorService;
        STATE_UPDATER.set(this, State.Stopped);
    }

    @Override
    public Type getType() {
        return routerType;
    }

    @Override
    public void setExchange(AmqpExchange exchange) {
        this.exchange = exchange;
    }

    @Override
    public AmqpExchange getExchange() {
        return exchange;
    }

    @Override
    public void setQueue(AmqpQueue queue) {
        this.queue = queue;
    }

    @Override
    public AmqpQueue getQueue() {
        return queue;
    }

    @Override
    public void addBindingKey(String bindingKey) {
        this.bindingKeys.add(bindingKey);
    }

    @Override
    public Set<String> getBindingKey() {
        return bindingKeys;
    }

    @Override
    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    @Override
    public Map<String, Object> getArguments() {
        return arguments;
    }

    public static AmqpMessageRouter generateRouter(AmqpExchange.Type type,
                                                   ScheduledExecutorService scheduledExecutorService) {

        if (type == null) {
            return null;
        }

        switch (type) {
            case Direct:
                return new DirectMessageRouter(scheduledExecutorService);
            case Fanout:
                return new FanoutMessageRouter(scheduledExecutorService);
            case Topic:
                return new TopicMessageRouter(scheduledExecutorService);
            case Headers:
                return new HeadersMessageRouter(scheduledExecutorService);
            default:
                return null;
        }
    }

    @Override
    public CompletableFuture<Void> routingMessage(long ledgerId, long entryId,
                                                  String routingKey, Map<String, Object> properties) {
        CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
        if (isMatch(properties)) {
            try {
                return queue.writeIndexMessageAsync(exchange.getName(), ledgerId, entryId);
            } catch (Exception e) {
                log.error("Failed to route message.", e);
                completableFuture.completeExceptionally(e);
            }
        }
        return completableFuture;
    }

    public abstract boolean isMatch(Map<String, Object> properties);

    private String getRouterSummary() {
        if (exchange == null || queue == null) {
            return "";
        }
        if (routerSummary == null) {
            routerSummary = String.format("[%s] [%s -> %s]",
                    exchange.getTopic().getName(), exchange.getName(), queue.getName());
        }
        return routerSummary;
    }

    public void startRouter() {
        if (queue instanceof InMemoryQueue) {
            log.warn("Unsupported operation startRouter to InMemoryQueue.");
            return;
        }
        if (exchange instanceof InMemoryExchange) {
            log.warn("Unsupported operation startRouter to InMemoryExchange.");
            return;
        }

        if (STATE_UPDATER.get(AbstractAmqpMessageRouter.this).equals(State.Stopping)) {
            long waitTimeMs = startBackOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Waiting for producer close before attempting reconnect, retrying in {} s",
                        getRouterSummary(), waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startRouter, waitTimeMs, TimeUnit.MILLISECONDS);
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state.equals(State.Started)) {
                // already running
                if (log.isDebugEnabled()) {
                    log.debug("{} Router was already running.", getRouterSummary());
                }
            } else {
                log.debug("{} Router was already started. Router State: {}", getRouterSummary(), state);
            }
            return;
        }

        log.info("{} Router is starting.", getRouterSummary());

        String cursorName = exchange.getName() + "_" + queue.getName();
        ((PersistentTopic) exchange.getTopic()).getManagedLedger()
                .asyncOpenCursor(cursorName, PulsarApi.CommandSubscribe.InitialPosition.Earliest,
                        new AsyncCallbacks.OpenCursorCallback() {
                            @Override
                            public void openCursorComplete(ManagedCursor managedCursor, Object o) {
                                log.info("{} Open cursor succeed for route.", getRouterSummary());
                                AbstractAmqpMessageRouter.this.cursor = managedCursor;
                                readEntries();
                            }

                            @Override
                            public void openCursorFailed(ManagedLedgerException e, Object o) {
                                retryStartRouter(e);
                            }
                        }, null);
    }

    private void retryStartRouter(Throwable ex) {
        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
            long waitTimeMs = startBackOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Failed to start router, errorMsg: {}, retrying in {} s.",
                        getRouterSummary(), ex.getMessage(), waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startRouter, waitTimeMs, TimeUnit.MILLISECONDS);
        } else {
            log.error("{} Failed to start router, errorMsg: {}", getRouterSummary(), ex.getMessage(), ex);
        }
    }

    private void readEntries() {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();
        cursor.cancelPendingReadRequest();

        log.info("{} Router is started.", getRouterSummary());
        startBackOff.reset();
        // activate cursor: so, entries can be cached
        cursor.setActive();
        readMoreEntries();
    }

    private void readMoreEntries() {
        log.info("[readMoreEntries]");
        int availablePermits = getAvailablePermits();
        if (availablePermits > 0) {
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Schedule read of {} messages.", getRouterSummary(), availablePermits);
                    cursor.asyncReadEntriesOrWait(availablePermits, defaultReadMaxSizeBytes, this, null);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Not schedule read due to pending read. Messages to read {}.",
                            getRouterSummary(), availablePermits);
                }
            }
        } else {
            // no permits from rate limit
            scheduledExecutorService.schedule(this::readMoreEntries, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

    private int getAvailablePermits() {
        int availablePermits = routerQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} Router queue is full, availablePermits: {}, pause route.",
                        getRouterSummary(), availablePermits);
            }
            return 0;
        }
        return availablePermits;
    }

    @Override
    public void readEntriesComplete(List<Entry> list, Object o) {
        log.info("[readEntriesComplete] listSize: {}", list.size());
        for (Entry entry : list) {
            try {
                MessageImpl<byte[]> entryMsg = MessageImpl.deserialize(entry.getDataBuffer());
                Map<String, Object> props = entryMsg.getMessageBuilder().getPropertiesList().stream()
                        .collect(Collectors.toMap(PulsarApi.KeyValue::getKey, PulsarApi.KeyValue::getValue));
                if (isMatch(props)) {
                    PENDING_SIZE_UPDATER.incrementAndGet(this);
                    IndexMessage indexMessage = IndexMessage.create(
                            exchange.getName(), entry.getLedgerId(), entry.getEntryId());
                    MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(indexMessage);
                    ((PersistentQueue) queue).getIndexTopic().publishMessage(
                            MessageConvertUtils.messageToByteBuf(message), RouteMessageContext.create(this, entry));
                } else {
                    AbstractAmqpMessageRouter.this.cursor.asyncDelete(
                            entry.getPosition(),
                            AbstractAmqpMessageRouter.this,
                            entry.getPosition());
                }
            } catch (Exception e) {
                log.warn("Route message failed.", e);
            }
        }

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);

        readMoreEntries();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException e, Object o) {
        long waitTimeMs = readFailureBackoff.next();
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries from bookie failed, retrying in {} s", getRouterSummary(), waitTimeMs / 1000, e);
        }
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        scheduledExecutorService.schedule(this::readMoreEntries, waitTimeMs, TimeUnit.MILLISECONDS);
    }

     static class RouteMessageContext implements Topic.PublishContext {
        private AbstractAmqpMessageRouter messageRouter;
        private Entry entry;

        private final Recycler.Handle<RouteMessageContext> recyclerHandle;

        private RouteMessageContext(Recycler.Handle<RouteMessageContext> recyclerHandle) {
            this.recyclerHandle = recyclerHandle;
        }

        static RouteMessageContext create(AbstractAmqpMessageRouter messageRouter, Entry entry) {
            RouteMessageContext messageContext = RECYCLER.get();
            messageContext.messageRouter = messageRouter;
            messageContext.entry = entry;
            return messageContext;
        }

        private void recycle() {
            messageRouter = null;
            entry = null;
            recyclerHandle.recycle(this);
        }

        private static final Recycler<RouteMessageContext> RECYCLER = new Recycler<RouteMessageContext>() {
            @Override
            protected RouteMessageContext newObject(Handle<RouteMessageContext> handle) {
                return new RouteMessageContext(handle);
            }
        };

        @Override
        public void completed(Exception exception, long ledgerId, long entryId) {
            if (exception != null) {
                log.error("{} Error producing messages", messageRouter.getRouterSummary(), exception);
                messageRouter.cursor.rewind();
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Route message successfully.", messageRouter.getRouterSummary());
                }
                messageRouter.cursor.asyncDelete(entry.getPosition(), messageRouter, entry.getPosition());
            }
            entry.release();

            int pending = PENDING_SIZE_UPDATER.decrementAndGet(messageRouter);
            if (pending == 0 && HAVE_PENDING_READ_UPDATER.get(messageRouter) == FALSE) {
                messageRouter.readMoreEntries();
            }

            recycle();
        }
    }

    @Override
    public void deleteComplete(Object position) {
        if (log.isDebugEnabled()) {
            log.debug("{} Deleted message at {}", getRouterSummary(), position);
        }
    }

    @Override
    public void deleteFailed(ManagedLedgerException e, Object position) {
        log.error("{} Failed to delete message at {}: {}", getRouterSummary(), position, e.getMessage(), e);
    }
}
