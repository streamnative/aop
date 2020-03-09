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
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLongLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * This method delivers a message to the client, via a consumer. In
 * the asynchronous message delivery model, the client starts a
 * consumer using the BasicConsume method, then the server responds with
 * BasicDeliver methods as and when messages arrive for that consumer.
 *
 */
public final class BasicDeliver extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(60);
    protected static final String METHOD_NAME = "deliver";

    private ShortString consumerTag;
    private UnsignedLongLong deliveryTag;
    private Bit redelivered;
    private ShortString exchange;
    private ShortString routingKey;

    public BasicDeliver(ByteBuf channelBuffer) {
        this(new ShortString(channelBuffer),
                new UnsignedLongLong(channelBuffer),
                new Bit(channelBuffer, 0),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer));
    }

    public BasicDeliver(ShortString consumerTag,
                        UnsignedLongLong deliveryTag,
                        Bit redelivered,
                        ShortString exchange,
                        ShortString routingKey) {
        this.consumerTag = consumerTag;
        this.deliveryTag = deliveryTag;
        this.redelivered = redelivered;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public ShortString getConsumerTag() {
        return this.consumerTag;
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

}
