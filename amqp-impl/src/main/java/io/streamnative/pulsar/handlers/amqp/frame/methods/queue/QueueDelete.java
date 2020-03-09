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
 * This method deletes a queue. When a queue is deleted any pending
 * messages are sent to a dead-letter queue if this is defined in the
 * server configuration, and all consumers on the queue are cancelled.
 */
@Getter
public final class QueueDelete extends Queue {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(40);
    protected static final String METHOD_NAME = "delete";

    private UnsignedShort ticket;
    private ShortString queue;
    private Bit ifUnused;
    private Bit ifEmpty;
    private Bit nowait;

    public QueueDelete(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new Bit(channelBuffer, 1),
                new Bit(channelBuffer, 2));
    }

    public QueueDelete(UnsignedShort ticket, ShortString queue, Bit ifUnused, Bit ifEmpty, Bit nowait) {
        this.ticket = ticket;
        this.queue = queue;
        this.ifUnused = ifUnused;
        this.ifEmpty = ifEmpty;
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
