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
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLongLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;


/**
 * This method allows a client to reject a message. It can be used to
 * interrupt and cancel large incoming messages, or return untreatable
 * messages to their original queue.
 */
public final class BasicReject extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(90);
    protected static final String METHOD_NAME = "reject";

    private UnsignedLongLong deliveryTag;
    private Bit requeue;

    public BasicReject(ByteBuf channelBuffer) {
        this(new UnsignedLongLong(channelBuffer), new Bit(channelBuffer, 0));
    }

    public BasicReject(UnsignedLongLong deliveryTag, Bit requeue) {
        this.deliveryTag = deliveryTag;
        this.requeue = requeue;
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

    public Bit getRequeue() {
        return this.requeue;
    }
}
