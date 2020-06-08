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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Amqp exchange reader, read entries from BookKeeper and process entries.
 */
@Slf4j
public abstract class AmqpExchangeReader implements AsyncCallbacks.ReadEntriesCallback, AsyncCallbacks.DeleteCallback {

    private PersistentExchange persistentExchange;
    private final String cursorNamePre = "__amqp_reader__";
    private String name;
    private PersistentTopic topic;
    private ManagedCursor cursor;
    private ScheduledExecutorService scheduledExecutorService;

    protected final Backoff startBackOff = new Backoff(
            100, TimeUnit.MILLISECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    private static final AtomicReferenceFieldUpdater<AmqpExchangeReader, AmqpExchangeReader.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(AmqpExchangeReader.class, AmqpExchangeReader.State.class, "state");
    private volatile AmqpExchangeReader.State state  = AmqpExchangeReader.State.Stopped;
    /**
     * Reader state.
     */
    protected enum State {
        Stopped, Starting, Started, Stopping
    }

    private static final int defaultReadMaxSizeBytes = 4 * 1024 * 1024;
    private static final int readerQueueSize = 1000;
    private volatile int pendingQueueSize = 0;
    private static final AtomicIntegerFieldUpdater<AmqpExchangeReader> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReader.class, "pendingQueueSize");

    private static final int FALSE = 0;
    private static final int TRUE = 1;

    private static final AtomicIntegerFieldUpdater<AmqpExchangeReader> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AmqpExchangeReader.class, "havePendingRead");
    private volatile int havePendingRead = FALSE;

    private final Backoff readFailureBackoff = new Backoff(
            1, TimeUnit.SECONDS, 1, TimeUnit.MINUTES, 0, TimeUnit.MILLISECONDS);

    protected AmqpExchangeReader(PersistentExchange persistentExchange) {
        this.persistentExchange = persistentExchange;
        this.topic = (PersistentTopic) persistentExchange.getTopic();
        this.scheduledExecutorService = topic.getBrokerService().executor();
        STATE_UPDATER.set(this, AmqpExchangeReader.State.Stopped);
        this.name = "[AMQP Reader for " + topic.getName() + " ]";
    }

    public void startReader() {
        if (STATE_UPDATER.get(AmqpExchangeReader.this).equals(AmqpExchangeReader.State.Stopping)) {
            long waitTimeMs = startBackOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Waiting for producer close before attempting reconnect, retrying in {} s",
                        name, waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReader, waitTimeMs, TimeUnit.MILLISECONDS);
        }
        AmqpExchangeReader.State state = STATE_UPDATER.get(this);
        if (!STATE_UPDATER.compareAndSet(this, AmqpExchangeReader.State.Stopped, AmqpExchangeReader.State.Starting)) {
            if (state.equals(AmqpExchangeReader.State.Started)) {
                // already running
                if (log.isDebugEnabled()) {
                    log.debug("{} Reader was already running.", name);
                }
            } else {
                log.debug("{} Reader was already started. Reader State: {}", name, state);
            }
            return;
        }

        log.info("{} Reader is starting.", name);

        topic.getManagedLedger().asyncOpenCursor(cursorNamePre + persistentExchange.getName(),
                PulsarApi.CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor managedCursor, Object o) {
                        log.info("{} Open cursor succeed for route.", name);
                        AmqpExchangeReader.this.cursor = managedCursor;
                        readEntries();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException e, Object o) {
                        retryStartReader(e);
                    }
                }, null);
    }

    private void retryStartReader(Throwable ex) {
        if (STATE_UPDATER.compareAndSet(this, AmqpExchangeReader.State.Starting, AmqpExchangeReader.State.Stopped)) {
            long waitTimeMs = startBackOff.next();
            if (log.isDebugEnabled()) {
                log.debug("{} Failed to start reader, errorMsg: {}, retrying in {} s.",
                        name, ex.getMessage(), waitTimeMs / 1000);
            }
            scheduledExecutorService.schedule(this::startReader, waitTimeMs, TimeUnit.MILLISECONDS);
        } else {
            log.error("{} Failed to start reader, errorMsg: {}", name, ex.getMessage(), ex);
        }
    }

    private void readEntries() {
        // Rewind the cursor to be sure to read again all non-acked messages sent while restarting
        cursor.rewind();
        cursor.cancelPendingReadRequest();

        log.info("{} Reader is started.", name);
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
                    log.debug("{} Schedule read of {} messages.", name, availablePermits);
                    cursor.asyncReadEntriesOrWait(availablePermits, defaultReadMaxSizeBytes, this, null);
                }
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
        int availablePermits = readerQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} Reader queue is full, availablePermits: {}, pause route.",
                        name, availablePermits);
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
                PENDING_SIZE_UPDATER.incrementAndGet(this);
                CompletableFuture<Void> completableFuture = readProcess(entry);
                completableFuture.whenComplete((ignored, exception) -> {
                    if (exception != null) {
                        log.error("{} Error producing messages", name, exception);
                        AmqpExchangeReader.this.cursor.rewind();
                    } else {
                        if (log.isDebugEnabled()) {
                            log.debug("{} Route message successfully.", name);
                        }
                        AmqpExchangeReader.this.cursor
                                .asyncDelete(entry.getPosition(), this, entry.getPosition());
                    }
                    entry.release();

                    int pending = PENDING_SIZE_UPDATER.decrementAndGet(this);
                    if (pending == 0 && HAVE_PENDING_READ_UPDATER.get(this) == FALSE) {
                        AmqpExchangeReader.this.readMoreEntries();
                    }
                });
            } catch (Exception e) {
                PENDING_SIZE_UPDATER.decrementAndGet(this);
                log.warn("Route message failed.", e);
            }
        }

        HAVE_PENDING_READ_UPDATER.set(this, FALSE);

        readMoreEntries();
    }

    public abstract CompletableFuture<Void> readProcess(Entry entry);

    @Override
    public void readEntriesFailed(ManagedLedgerException e, Object o) {
        long waitTimeMs = readFailureBackoff.next();
        if (log.isDebugEnabled()) {
            log.debug("{} Read entries from bookie failed, retrying in {} s", name, waitTimeMs / 1000, e);
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

}
