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
 * This method publishes a message to a specific exchange. The message
 * will be routed to queues as defined by the exchange configuration
 * and distributed to any active consumers when the transaction, if any,
 * is committed.
 */
public final class BasicPublish extends Basic {
    public static final UnsignedShort METHOD_ID = new UnsignedShort(40);
    protected static final String METHOD_NAME = "publish";

    private UnsignedShort ticket;
    private ShortString exchange;
    private ShortString routingKey;
    private Bit mandatory;
    private Bit immediate;

    public BasicPublish(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new Bit(channelBuffer, 1));
    }

    public BasicPublish(UnsignedShort ticket,
                        ShortString exchange,
                        ShortString routingKey,
                        Bit mandatory,
                        Bit immediate) {
        this.ticket = ticket;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.mandatory = mandatory;
        this.immediate = immediate;
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

    public ShortString getExchange() {
        return this.exchange;
    }

    public ShortString getRoutingKey() {
        return this.routingKey;
    }

    public Bit getMandatory() {
        return this.mandatory;
    }

    public Bit getImmediate() {
        return this.immediate;
    }

}
