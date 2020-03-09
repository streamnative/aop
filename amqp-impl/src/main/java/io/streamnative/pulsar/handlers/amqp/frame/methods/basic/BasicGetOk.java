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
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLongLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * This method delivers a message to the client following a get
 * method. A message delivered by 'get-ok' must be acknowledged
 * unless the no-ack option was set in the get method.
 *
 */
public final class BasicGetOk extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(71);
    protected static final String METHOD_NAME = "get-ok";

    private UnsignedLongLong deliveryTag;
    private Bit redelivered;
    private ShortString exchange;
    private ShortString routingKey;
    private UnsignedLong messageCount;

    public BasicGetOk(ByteBuf channelBuffer) {
        this(new UnsignedLongLong(channelBuffer),
                new Bit(channelBuffer, 0),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new UnsignedLong(channelBuffer));
    }

    public BasicGetOk(UnsignedLongLong deliveryTag,
                      Bit redelivered,
                      ShortString exchange,
                      ShortString routingKey,
                      UnsignedLong messageCount) {
        this.deliveryTag = deliveryTag;
        this.redelivered = redelivered;
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.messageCount = messageCount;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public UnsignedLongLong getDeliveryTag() {
        return this.deliveryTag;
    }

    public Bit getRedelivered() {
        return this.redelivered;
    }

    public ShortString getExchange() {
        return this.exchange;
    }

    public ShortString getRoutingKey() {
        return this.routingKey;
    }

    public UnsignedLong getMessageCount() {
        return this.messageCount;
    }

}
