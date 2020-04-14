/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.extern.log4j.Log4j2;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

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

        return cursors.computeIfAbsent(name, cusrsor -> {

            ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
            PositionImpl position = (PositionImpl) ledger.getLastConfirmedEntry();
            if (log.isDebugEnabled()) {
                log.debug("Create cursor {} for offset: {}. position:{}", name, position);
            }
            ManagedCursor newCursor;
            try {
                newCursor = ledger.newNonDurableCursor(position, name);
                cursors.put(newCursor.getName(), newCursor);
            } catch (ManagedLedgerException e) {
                log.error("Error new cursor for topic {} at postion {} - {}. will cause fetch data error.",
                    topic.getName(), position, e);
                return null;
            }

            return newCursor;

        });

    }

    @Override
    public void close() throws IOException {

    }
}
