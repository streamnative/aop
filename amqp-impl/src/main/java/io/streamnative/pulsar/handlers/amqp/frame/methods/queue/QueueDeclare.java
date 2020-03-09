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
 * This method creates or checks a queue. When creating a new queue
 * the client can specify various properties that control the durability
 * of the queue and its contents, and the level of sharing for the queue.
 */
@Getter
public final class QueueDeclare extends Queue {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(10);
    protected static final String METHOD_NAME = "declare";

    private UnsignedShort ticket;
    private ShortString queue;
    private Bit passive;
    private Bit durable;
    private Bit exclusive;
    private Bit autoDelete;
    private Bit nowait;
    private FieldTable arguments;

    public QueueDeclare(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer), new ShortString(channelBuffer),
                new Bit(channelBuffer, 0), new Bit(channelBuffer, 1),
                new Bit(channelBuffer, 2), new Bit(channelBuffer, 3),
                new Bit(channelBuffer, 4), new FieldTable(channelBuffer));
    }

    public QueueDeclare(UnsignedShort ticket, ShortString queue,
                        Bit passive, Bit durable,
                        Bit exclusive, Bit autoDelete,
                        Bit nowait, FieldTable arguments) {
        this.ticket = ticket;
        this.queue = queue;
        this.passive = passive;
        this.durable = durable;
        this.exclusive = exclusive;
        this.autoDelete = autoDelete;
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
