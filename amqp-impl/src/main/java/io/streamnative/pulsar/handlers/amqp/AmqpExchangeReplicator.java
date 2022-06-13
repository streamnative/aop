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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Amqp exchange replicator, read entries from BookKeeper and process entries.
 */
@Slf4j
public class AmqpExchangeReplicator implements AsyncCallbacks.ReadEntriesCallback, AsyncCallbacks.DeleteCallback {

    private final PersistentExchange persistentExchange;
    private static final String CURSOR_NAME_PREFIX = "__amqp_replicator__";
    private final String name;
    private final PersistentTopic topic;
    private ManagedCursor cursor;
    private final ScheduledExecutorService scheduledExecutorService;

    protected final Backoff backOff = new Backoff(
            100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private final AtomicReferenceFieldUpdater<AmqpExchangeReplicator, State> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(AmqpExchangeReplicator.class, State.class, "state");
    private volatile State state = State.Stopped;
    /**
     * Replicator state.
     */
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final int defaultReadMaxSizeBytes = 5 * 1024 * 1024;
    private int routeQueueSize = 200;
    private volatile int pendingQueueSize = 0;
    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "pendingQueueSize");

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Backoff readFailureBackoff = new Backoff(
            1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private final Executor routeExecutor;

    public AmqpExchangeReplicator(PersistentExchange persistentExchange, Executor routeExecutor,
                                     int routeQueueSize) {
        this.persistentExchange = persistentExchange;
        this.topic = (PersistentTopic) persistentExchange.getTopic();
        this.scheduledExecutorService = topic.getBrokerService().executor();
        this.initMaxRouteQueueSize(routeQueueSize);
        this.routeExecutor = routeExecutor;
        stateUpdater.set(this, AmqpExchangeReplicator.State.Stopped);
        this.name = "[AMQP Replicator for " + topic.getName() + " ]";
    }

    private void initMaxRouteQueueSize(int maxRouteQueueSize) {
        if (System.getProperty("aop.replicatorQueueSize") != null) {
            this.routeQueueSize = Integer.parseInt(
                    System.getProperty("aop.replicatorQueueSize", "" + maxRouteQueueSize));
            return;
        }
        this.routeQueueSize = maxRouteQueueSize;
    }

    public void startReplicate() {
        if (stateUpdater.get(AmqpExchangeReplicator.this).equals(AmqpExchangeReplicator.State.Stopping)) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Waiting for producer close before attempting reconnect, retrying in {} s",
                        name, waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
            return;
        }
        State state = stateUpdater.get(this);
        if (!stateUpdater.compareAndSet(this, State.Stopped, State.Starting)) {
            if (state.equals(State.Started)) {
                // already running
                if (log.isDebugEnabled()) {
                    log.debug("{} Replicator was already running.", name);
                }
            } else {
                log.debug("{} Replicator was already started. Replicator State: {}", name, state);
            }
            return;
        }

        log.info("{} Replicator is starting.", name);

        topic.getManagedLedger().asyncOpenCursor(CURSOR_NAME_PREFIX + persistentExchange.getName(),
                CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor managedCursor, Object o) {
                        AmqpExchangeReplicator.this.cursor = managedCursor;
                        startReadEntries();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException e, Object o) {
                        retryStartReplicator(e);
                    }
                }, null);
    }

    private void retryStartReplicator(Throwable ex) {
        if (stateUpdater.compareAndSet(this, State.Starting, State.Stopped)) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Failed to start replicator, errorMsg: {}, retrying in {} s.",
                        name, ex.getMessage(), waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
        } else {
            log.error("{} Failed to start replicator, errorMsg: {}", name, ex.getMessage(), ex);
        }
    }

    private void startReadEntries() {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();
        cursor.cancelPendingReadRequest();

        backOff.reset();
        // activate cursor: so, entries can be cached
        cursor.setActive();

        stateUpdater.set(this, State.Started);
        log.info("{} Replicator is started, routeQueueSize: {}.", name, this.routeQueueSize);

        readMoreEntries();
    }

    private void readMoreEntries() {
        if (log.isDebugEnabled()) {
            log.debug("{} Read more entries.", name);
        }
        int availablePermits = getAvailablePermits();
        if (availablePermits > 0) {
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Schedule read of {} messages.", name, availablePermits);
                }
                cursor.asyncReadEntriesOrWait(availablePermits, defaultReadMaxSizeBytes, this, null, null);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Not schedule read due to pending read. Messages to read {}.",
                            name, availablePermits);
                }
            }
        } else {
            // no permits from rate limit
            scheduledExecutorService.schedule(this::readMoreEntries, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

    private int getAvailablePermits() {
        int availablePermits = routeQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} Replicator queue is full, availablePermits: {}, pause route.",
                        name, availablePermits);
            }
            return 0;
        }
        return availablePermits;
    }

    @Override
    public void readEntriesComplete(List<Entry> list, Object o) {
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries complete. Entries size: {}", name, list.size());
        }
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        if (list == null || list.isEmpty()) {
            long delay = readFailureBackoff.next();
            log.warn("{} The read entry list is empty, will retry in {} ms. ReadPosition: {}, LAC: {}.",
                    name, delay, cursor.getReadPosition(), topic.getManagedLedger().getLastConfirmedEntry());
            scheduledExecutorService.schedule(this::readMoreEntries, delay, TimeUnit.MILLISECONDS);
            return;
        }
        readFailureBackoff.reduceToHalf();
        List<Pair<PositionImpl, ByteBuf>> bufList = new ArrayList<>(list.size());
        for (Entry entry : list) {
            bufList.add(
                    Pair.of(PositionImpl.get(entry.getLedgerId(), entry.getEntryId()), entry.getDataBuffer()));
        }
        routeExecutor.execute(() -> this.readComplete(bufList));
    }

    private void readComplete(List<Pair<PositionImpl, ByteBuf>> list) {
        for (Pair<PositionImpl, ByteBuf> entry : list) {
            PENDING_SIZE_UPDATER.incrementAndGet(this);
            routeMessages(entry.getRight(), entry.getLeft()).whenCompleteAsync((ignored, exception) -> {
                if (exception != null) {
                    log.error("{} Failed to route messages", name, exception);
                    this.cursor.rewind();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Route message successfully.", name);
                    }
                    AmqpExchangeReplicator.this.cursor
                            .asyncDelete(entry.getLeft(), this, entry.getLeft());
                }
                if (PENDING_SIZE_UPDATER.decrementAndGet(this) < routeQueueSize * 0.5
                        && HAVE_PENDING_READ_UPDATER.get(this) == FALSE) {
                    this.readMoreEntries();
                }
            }, routeExecutor);
            entry.getRight().release();
        }
    }

    private CompletableFuture<Void> routeMessages(ByteBuf data, Position position) {
        Map<String, Object> props;
        try {
            MessageImpl<byte[]> message = MessageImpl.deserialize(data);
            props = message.getMessageBuilder().getPropertiesList().stream()
                    .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
        } catch (IOException e) {
            log.error("Failed to deserialize entry data for exchange {}", persistentExchange.getName(), e);
            return FutureUtil.failedFuture(e);
        }
        List<CompletableFuture<Void>> routeFutureList = new ArrayList<>();
        for (AmqpQueue queue : persistentExchange.getQueues()) {
            CompletableFuture<Void> routeFuture = queue.getRouter(persistentExchange.getName()).routingMessage(
                    position.getLedgerId(), position.getEntryId(),
                    props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString(),
                    props);
            routeFutureList.add(routeFuture);
        }
        return FutureUtil.waitForAll(routeFutureList);
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object o) {
        if (exception instanceof ManagedLedgerException.CursorAlreadyClosedException) {
            log.error("[{}] Error reading entries because cursor is already closed.", name, exception);
            cursor.setInactive();
            cursor = null;
            stopReplicate();
            return;
        }

        long waitTimeMs = readFailureBackoff.next();
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries from bookie failed, retrying in {} s", name, waitTimeMs / 1000, exception);
        }
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        scheduledExecutorService.schedule(this::readMoreEntries, waitTimeMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void deleteComplete(Object position) {
        if (log.isDebugEnabled()) {
            log.debug("{} Deleted message at {}", name, position);
        }
    }

    @Override
    public void deleteFailed(ManagedLedgerException e, Object position) {
        log.error("{} Failed to delete message at {}: {}", name, position, e.getMessage(), e);
    }

    public void stopReplicate() {
        if (cursor == null) {
            stateUpdater.set(this, State.Stopped);
            log.info("[{}] AMQP Exchange Replicator is stopped.", name);
            return;
        }

        if (stateUpdater.get(this).equals(State.Stopping)) {
            log.warn("Replicator is stopping.");
            return;
        }

        if (cursor != null && (stateUpdater.compareAndSet(this, State.Starting, State.Stopping)
                || stateUpdater.compareAndSet(this, State.Started, State.Stopping))) {
            cursor.setInactive();
            cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object o) {
                    log.info("[{}] AMQP Exchange Replicator is stopped.", name);
                    stateUpdater.set(AmqpExchangeReplicator.this, State.Stopped);
                    cursor = null;
                }

                @Override
                public void closeFailed(ManagedLedgerException e, Object o) {
                    if (e instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                        cursor = null;
                        stateUpdater.set(AmqpExchangeReplicator.this, State.Stopped);
                        return;
                    }
                    long waitTimeMs = backOff.next();
                    log.error("[{}] AMQP Exchange Replicator stop failed. retrying in {} s",
                            name, waitTimeMs / 1000, e);
                    AmqpExchangeReplicator.this.scheduledExecutorService.schedule(
                            () -> AmqpExchangeReplicator.this.stopReplicate(),
                            waitTimeMs, TimeUnit.MILLISECONDS);
                }
            }, null);
        }
    }

}
