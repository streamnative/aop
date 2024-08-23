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

import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.util.Backoff;

/**
 * Amqp exchange replicator, read entries from BookKeeper and process entries.
 */
@Slf4j
public abstract class AmqpExchangeReplicator implements AsyncCallbacks.ReadEntriesCallback,
        AsyncCallbacks.DeleteCallback {

    private PersistentExchange persistentExchange;
    private final String cursorNamePre = "__amqp_replicator__";
    private String name;
    private PersistentTopic topic;
    private ManagedCursor cursor;
    private ScheduledExecutorService scheduledExecutorService;

    protected final Backoff backOff = new Backoff(
            100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private static final AtomicReferenceFieldUpdater<AmqpExchangeReplicator, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AmqpExchangeReplicator.class, State.class, "state");
    private volatile State state  = State.Stopped;
    /**
     * Replicator state.
     */
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    private int routeQueueSize = 200;
    private volatile int pendingQueueSize = 0;
    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "pendingQueueSize");
    private int readBatchSize;
    private final int readMaxSizeBytes;
    private final int readMaxBatchSize;

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<AmqpExchangeReplicator> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReplicator.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Backoff readFailureBackoff = new Backoff(
            1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private final ExecutorService routeExecutor;

    protected AmqpExchangeReplicator(PersistentExchange persistentExchange, ExecutorService routeExecutor,
                                     int routeQueueSize) {
        this.persistentExchange = persistentExchange;
        this.topic = (PersistentTopic) persistentExchange.getTopic();
        this.scheduledExecutorService = topic.getBrokerService().executor();
        this.initMaxRouteQueueSize(routeQueueSize);
        this.routeExecutor = routeExecutor;
        this.readMaxBatchSize = Math.min(this.routeQueueSize,
                topic.getBrokerService().getPulsar().getConfig().getDispatcherMaxReadBatchSize());
        this.readBatchSize = this.readMaxBatchSize;
        this.readMaxSizeBytes = topic.getBrokerService().getPulsar().getConfig().getDispatcherMaxReadSizeBytes();
        STATE_UPDATER.set(this, AmqpExchangeReplicator.State.Stopped);
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
        if (STATE_UPDATER.get(AmqpExchangeReplicator.this).equals(AmqpExchangeReplicator.State.Stopping)) {
            long waitTimeMs = backOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Waiting for producer close before attempting reconnect, retrying in {} s",
                        name, waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
        }
        State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, State.Stopped, State.Starting)) {
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

        topic.getManagedLedger().asyncOpenCursor(cursorNamePre + persistentExchange.getName(),
                CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor managedCursor, Object o) {
                        log.info("{} Open cursor succeed for route.", name);
                        AmqpExchangeReplicator.this.cursor = managedCursor;
                        readEntries();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException e, Object o) {
                        retryStartReplicator(e);
                    }
                }, null);
    }

    private void retryStartReplicator(Throwable ex) {
        if (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopped)) {
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

    private void readEntries() {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();
        cursor.cancelPendingReadRequest();

        backOff.reset();
        // activate cursor: so, entries can be cached
        cursor.setActive();

        STATE_UPDATER.set(this, State.Started);
        log.info("{} Replicator is started, routeQueueSize: {}.", name, this.routeQueueSize);

        readMoreEntries();
    }

    private void readMoreEntries() {
        if (log.isDebugEnabled()) {
            log.debug("{} Read more entries.", name);
        }
        int availablePermits = getAvailablePermits();
        if (availablePermits > 0) {
            int messagesToRead = Math.min(availablePermits, readBatchSize);
            // avoid messageToRead is 0
            messagesToRead = Math.max(messagesToRead, 1);

            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                log.info("{} Schedule read of {} messages.", name, messagesToRead);
                if (log.isDebugEnabled()) {
                    log.debug("{} Schedule read of {} messages.", name, messagesToRead);
                }
                cursor.asyncReadEntriesOrWait(messagesToRead, readMaxSizeBytes, this, null, PositionImpl.LATEST);
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

        if (topic.getDispatchRateLimiter().isPresent()
                && topic.getDispatchRateLimiter().get().isDispatchRateLimitingEnabled()) {
            long availableOnByte = topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnByte();
            long availableOnMsg = topic.getDispatchRateLimiter().get().getAvailableDispatchRateLimitOnMsg();
            if (availableOnByte == 0 || availableOnMsg == 0) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Dispatch rate limit is reached, availableOnByte: {}, availableOnMsg: {}.",
                            name, availableOnByte, availableOnMsg);
                }
                return -1;
            }
            if (availableOnMsg > 0) {
                availablePermits = Math.min(availablePermits, (int) availableOnMsg);
            }
        }

        return availablePermits;
    }

    @Override
    public void readEntriesComplete(List<Entry> list, Object o) {
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries complete. Entries size: {}", name, list.size());
        }
        HAVE_PENDING_READ_UPDATER.set(this, FALSE);
        if (CollectionUtils.isEmpty(list)) {
            long delay = readFailureBackoff.next();
            log.warn("{} The read entry list is empty, will retry in {} ms. ReadPosition: {}, LAC: {}.",
                    name, delay, cursor.getReadPosition(), topic.getManagedLedger().getLastConfirmedEntry());
            scheduledExecutorService.schedule(this::readMoreEntries, delay, TimeUnit.MILLISECONDS);
            return;
        }

        if (readBatchSize < readMaxBatchSize) {
            int newReadBatchSize = Math.min(readBatchSize * 2, readMaxBatchSize);
            if (log.isDebugEnabled()) {
                log.debug("[{}] Increasing read batch size from {} to {}", name, readBatchSize,
                        newReadBatchSize);
            }

            readBatchSize = newReadBatchSize;
        }

        readFailureBackoff.reduceToHalf();
        routeExecutor.execute(() -> this.handleEntries(list));
    }

    private void handleEntries(List<Entry> list) {
        PENDING_SIZE_UPDATER.addAndGet(this, list.size());

        List<Pair<Position, Map<String, Object>>> propsList = new ArrayList<>();
        boolean encounterError = false;
        for (Entry entry : list) {
            if (encounterError) {
                entry.release();
                continue;
            }

            Map<String, Object> props;
            try {
                MessageImpl<byte[]> message = MessageImpl.deserialize(entry.getDataBuffer());
                props = message.getMessageBuilder().getPropertiesList().stream()
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
            } catch (Exception e) {
                log.error("Failed to deserialize entry dataBuffer for topic: {}, rewind cursor.", name, e);
                encounterError = true;
                propsList.clear();
                continue;
            }

            propsList.add(Pair.of(entry.getPosition(), props));
            topic.getDispatchRateLimiter().ifPresent(
                    limiter -> limiter.consumeDispatchQuota(1, entry.getLength()));
            entry.release();
        }
        if (encounterError) {
            cursor.rewind();
            PENDING_SIZE_UPDATER.set(this, 0);
            this.readMoreEntries();
            return;
        }

        for (var posAndProps : propsList) {
            final Position position = posAndProps.getLeft();
            routeIndex(posAndProps.getRight(), position).whenCompleteAsync((ignored, exception) -> {
                if (exception != null) {
                    log.error("{} Error producing messages", name, exception);
                    this.cursor.rewind();
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("{} Route message successfully.", name);
                    }
                    AmqpExchangeReplicator.this.cursor.asyncDelete(position, this, position);
                }
                if (PENDING_SIZE_UPDATER.decrementAndGet(this) <= 0) {
                    this.readMoreEntries();
                }
            }, routeExecutor);
        }
    }

    public abstract CompletableFuture<Void> routeIndex(Map<String, Object> props, Position position);

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
        readBatchSize = topic.getBrokerService().pulsar().getConfiguration().getDispatcherMinReadBatchSize();
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
            STATE_UPDATER.set(this, State.Stopped);
            log.info("[{}] AMQP Exchange Replicator is stopped.", name);
            return;
        }

        if (STATE_UPDATER.get(this).equals(State.Stopping)) {
            log.warn("Replicator is stopping.");
            return;
        }

        if (cursor != null && (STATE_UPDATER.compareAndSet(this, State.Starting, State.Stopping)
                || STATE_UPDATER.compareAndSet(this, State.Started, State.Stopping))) {
            cursor.setInactive();
            cursor.asyncClose(new AsyncCallbacks.CloseCallback() {
                @Override
                public void closeComplete(Object o) {
                    log.info("[{}] AMQP Exchange Replicator is stopped.", name);
                    STATE_UPDATER.set(AmqpExchangeReplicator.this, State.Stopped);
                    cursor = null;
                }

                @Override
                public void closeFailed(ManagedLedgerException e, Object o) {
                    if (e instanceof ManagedLedgerException.CursorAlreadyClosedException) {
                        cursor = null;
                        STATE_UPDATER.set(AmqpExchangeReplicator.this, State.Stopped);
                        return;
                    }
                    long waitTimeMs = backOff.next();
                    log.error("[{}] AMQP Exchange Replicator stop failed. retrying in {} s",
                            name, waitTimeMs / 1000, e);
                    AmqpExchangeReplicator.this.scheduledExecutorService.schedule(
                            AmqpExchangeReplicator.this::stopReplicate, waitTimeMs, TimeUnit.MILLISECONDS);
                }
            }, null);
        }

        log.info("[{}] AMQP Exchange Replicator is already stopped. State: {}", name, STATE_UPDATER.get(this));
    }

}
