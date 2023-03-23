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
package io.streamnative.pulsar.handlers.amqp.flow;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import org.apache.qpid.server.flow.FlowCreditManager;

/**
 * AmqpFlowCreditManager flow control manager.
 */
public class AmqpFlowCreditManager implements FlowCreditManager {

    private volatile long bytesCreditLimit;
    @Getter
    private volatile long messageCreditLimit;

    private volatile long bytesCredit;
    private volatile long messageCredit;
    private final ReentrantReadWriteLock rwLock;

    public AmqpFlowCreditManager(long bytesCreditLimit, long messageCreditLimit) {
        this.bytesCreditLimit = bytesCreditLimit;
        this.messageCreditLimit = messageCreditLimit;
        this.rwLock = new ReentrantReadWriteLock();
    }

    public void setCreditLimits(final long bytesCreditLimit, final long messageCreditLimit) {
        rwLock.writeLock().lock();
        try {
            long bytesCreditChange = bytesCreditLimit - this.bytesCreditLimit;
            long messageCreditChange = messageCreditLimit - this.messageCreditLimit;

            if (bytesCreditChange != 0L) {
                bytesCredit += bytesCreditChange;
            }

            if (messageCreditChange != 0L) {
                messageCredit += messageCreditChange;
            }

            this.bytesCreditLimit = bytesCreditLimit;
            this.messageCreditLimit = messageCreditLimit;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void restoreCredit(final long messageCredit, final long bytesCredit) {
        rwLock.writeLock().lock();
        try {
            if (messageCreditLimit != 0) {
                this.messageCredit += messageCredit;
                if (this.messageCredit > messageCreditLimit) {
                    throw new IllegalStateException(String.format("Consumer credit accounting "
                        + "error. Restored more credit than we ever had: messageCredit=%d  "
                        + "messageCreditLimit=%d", this.messageCredit, messageCreditLimit));
                }
            }

            if (bytesCreditLimit != 0) {
                this.bytesCredit += bytesCredit;
                if (this.bytesCredit > bytesCreditLimit) {
                    throw new IllegalStateException(String.format("Consumer credit accounting error.Restored more "
                            + "credit than we ever had: bytesCredit=%d bytesCreditLimit=%d",
                        this.bytesCredit, bytesCreditLimit));
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public boolean hasCredit() {
        rwLock.readLock().lock();
        try {
            return (bytesCreditLimit == 0L || bytesCredit > 0)
                && (messageCreditLimit == 0L || messageCredit > 0);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public boolean useCreditForMessages(final long messageCredit, final long msgSize) {
        rwLock.writeLock().lock();
        try {
            if (messageCreditLimit != 0) {
                if (messageCredit <= 0) {
                    return false;
                }
            }
            if (bytesCreditLimit != 0) {
                if ((bytesCredit < msgSize) && (bytesCredit != bytesCreditLimit)) {
                    return false;
                }
            }

            this.messageCredit -= messageCredit;
            this.bytesCredit -= msgSize;
            return true;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override public boolean useCreditForMessage(long l) {
        return false;
    }

    public long getMessageCredit() {
        return messageCredit;
    }

    public boolean isNoCreditLimit(){
        return bytesCreditLimit == 0L && messageCreditLimit == 0L;
    }
}
