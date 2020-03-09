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
package io.streamnative.pulsar.handlers.amqp.frame.methods.basic;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method asks the server to start a "consumer", which is a
 * transient request for messages from a specific queue. Consumers
 * last as long as the channel they were created on, or until the
 * client cancels them.
 */
@Getter
public final class BasicConsume extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(20);
    protected static final String METHOD_NAME = "consume";

    private UnsignedShort ticket;
    private ShortString queue;
    private ShortString consumerTag;
    private Bit noLocal;
    private Bit noAck;
    private Bit exclusive;
    private Bit nowait;

    public BasicConsume(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new Bit(channelBuffer, 1),
                new Bit(channelBuffer, 2),
                new Bit(channelBuffer, 3));
    }

    public BasicConsume(UnsignedShort ticket, ShortString queue,
                        ShortString consumerTag, Bit noLocal,
                        Bit noAck, Bit exclusive, Bit nowait) {
        this.ticket = ticket;
        this.queue = queue;
        this.consumerTag = consumerTag;
        this.noLocal = noLocal;
        this.noAck = noAck;
        this.exclusive = exclusive;
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
