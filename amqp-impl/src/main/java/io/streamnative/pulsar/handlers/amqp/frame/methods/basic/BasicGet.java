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

/**
 * This method provides a direct access to the messages in a queue
 * using a synchronous dialogue that is designed for specific types of
 * application where synchronous functionality is more important than
 * performance.
 */
public final class BasicGet extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(70);
    protected static final String METHOD_NAME = "get";

    private UnsignedShort ticket;
    private ShortString queue;
    private Bit noAck;

    public BasicGet(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0));
    }

    public BasicGet(UnsignedShort ticket, ShortString queue, Bit noAck) {
        this.ticket = ticket;
        this.queue = queue;
        this.noAck = noAck;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public UnsignedShort getTicket() {
        return this.ticket;
    }

    public ShortString getQueue() {
        return this.queue;
    }

    public Bit getNoAck() {
        return this.noAck;
    }

}
