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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.log4j.Log4j2;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * exchange topic and queue cursor manager.
 */
@Log4j2
public class AmqpTopicCursorManager implements Closeable {

    private final PersistentTopic topic;
    private final ReentrantReadWriteLock rwLock;
    private boolean closed;
    private final ConcurrentMap<String, ManagedCursor> cursors;

    public AmqpTopicCursorManager(PersistentTopic topic) {
        this.topic = topic;
        this.rwLock = new ReentrantReadWriteLock();
        this.closed = false;
        cursors = new ConcurrentHashMap<>();
        for (ManagedCursor cursor : topic.getManagedLedger().getCursors()) {
            cursors.put(cursor.getName(), cursor);
        }
    }

    public ManagedCursor getCursor(String name) {
        return cursors.get(name);
    }

    public ManagedCursor getOrCreateCursor(String name) {
        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            return cursors.computeIfAbsent(name, cusrsor -> {

                ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                if (log.isDebugEnabled()) {
                    log.debug("Create cursor {} for topic {}", name, topic.getName());
                }
                ManagedCursor newCursor;
                try {
                    newCursor = ledger.openCursor(name, PulsarApi.CommandSubscribe.InitialPosition.Latest);
                    cursors.put(newCursor.getName(), newCursor);
                } catch (ManagedLedgerException | InterruptedException e) {
                    log.error("Error new cursor for topic {} - {}. will cause fetch data error.",
                        topic.getName(), e);
                    return null;
                }

                return newCursor;

            });
        } finally {
            rwLock.readLock().unlock();
        }

    }

    public ManagedCursor deleteCursor(String cursorName) {
        rwLock.readLock().lock();
        try {
            if (closed) {
                return null;
            }
            ManagedCursor cursor = cursors.remove(cursorName);
            deleteCursorAsync(cursor);
            return cursor;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void deleteCursorAsync(ManagedCursor cursor) {
        if (cursor != null) {
            topic.getManagedLedger().asyncDeleteCursor(cursor.getName(), new AsyncCallbacks.DeleteCursorCallback() {
                @Override
                public void deleteCursorComplete(Object ctx) {
                    if (log.isDebugEnabled()) {
                        log.debug("Cursor {} for topic {} deleted successfully .", cursor.getName(), topic.getName());
                    }
                }

                @Override
                public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("[{}] Error deleting cursor {} for topic {} for reason: {}.",
                        cursor.getName(), topic.getName(), exception);
                }
            }, null);
        }

    }

    @Override
    public void close() throws IOException {

    }
}
