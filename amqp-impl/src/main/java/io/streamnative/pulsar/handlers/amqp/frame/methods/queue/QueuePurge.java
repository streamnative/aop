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
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method removes all messages from a queue. It does not cancel
 * consumers. Purged messages are deleted without any formal "undo"
 * mechanism.
 */
@Getter
public final class QueuePurge extends Queue {
    public static final UnsignedShort METHOD_ID = new UnsignedShort(30);
    protected static final String METHOD_NAME = "purge";

    private UnsignedShort ticket;
    private ShortString queue;
    private Bit nowait;

    public QueuePurge(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0));
    }

    public QueuePurge(UnsignedShort ticket, ShortString queue, Bit nowait) {
        this.ticket = ticket;
        this.queue = queue;
        this.nowait = nowait;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

}
