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
package io.streamnative.pulsar.handlers.amqp.frame.methods.exchange;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.FieldTable;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method creates an exchange if it does not already exist, and if the
 * exchange exists, verifies that it is of the correct and expected class.
 */
@Getter
public final class ExchangeDeclare extends Exchange {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(10);
    protected static final String METHOD_NAME = "declare";

    private UnsignedShort ticket;
    private ShortString exchange;
    private ShortString type;
    private Bit passive;
    private Bit durable;
    private Bit autoDelete;
    private Bit internal;
    private Bit nowait;
    private FieldTable arguments;

    public ExchangeDeclare(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new Bit(channelBuffer, 1),
                new Bit(channelBuffer, 2),
                new Bit(channelBuffer, 3),
                new Bit(channelBuffer, 4),
                new FieldTable(channelBuffer));
    }

    public ExchangeDeclare(UnsignedShort ticket, ShortString exchange,
                           ShortString type, Bit passive,
                           Bit durable, Bit autoDelete,
                           Bit internal, Bit nowait, FieldTable arguments) {
        this.ticket = ticket;
        this.exchange = exchange;
        this.type = type;
        this.passive = passive;
        this.durable = durable;
        this.autoDelete = autoDelete;
        this.internal = internal;
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
