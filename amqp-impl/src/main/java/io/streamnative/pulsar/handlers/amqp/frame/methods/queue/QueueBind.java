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
import io.streamnative.pulsar.handlers.amqp.frame.types.FieldTable;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method binds a queue to an exchange. Until a queue is
 * bound it will not receive any messages. In a classic messaging
 * model, store-and-forward queues are bound to a dest exchange
 * and subscription queues are bound to a dest_wild exchange.
 */
@Getter
public final class QueueBind extends Queue {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(20);
    protected static final String METHOD_NAME = "bind";

    private UnsignedShort ticket;
    private ShortString queue;
    private ShortString exchange;
    private ShortString routingKey;
    private Bit nowait;
    private FieldTable arguments;

    public QueueBind(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new FieldTable(channelBuffer));
    }

    public QueueBind(UnsignedShort ticket, ShortString queue, ShortString exchange,
                     ShortString routingKey, Bit nowait, FieldTable arguments) {
        this.ticket = ticket;
        this.queue = queue;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.nowait = nowait;
        this.arguments = arguments;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
