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
package io.streamnative.pulsar.handlers.amqp.frame.methods.queue;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * This method confirms the purge of a queue.
 */
public final class QueuePurgeOk extends Queue {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(31);
    protected static final String METHOD_NAME = "purge-ok";

    private UnsignedLong messageCount;

    public QueuePurgeOk(ByteBuf channelBuffer) {
        this(new UnsignedLong(channelBuffer));
    }

    public QueuePurgeOk(UnsignedLong messageCount) {
        this.messageCount = messageCount;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public UnsignedLong getMessageCount() {
        return this.messageCount;
    }
}
