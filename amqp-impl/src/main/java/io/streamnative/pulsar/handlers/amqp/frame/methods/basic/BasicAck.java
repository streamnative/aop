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
import lombok.Getter;

/**
 * This method acknowledges one or more messages delivered via the
 * BasicDeliver or Get-Ok methods. The client can ask to confirm a
 * single message or a set of messages up to and including a specific
 * message.
 */
@Getter
public final class BasicAck extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(80);
    protected static final String METHOD_NAME = "ack";

    private UnsignedLongLong deliveryTag;
    private Bit multiple;

    public BasicAck(ByteBuf channelBuffer) {
        this(new UnsignedLongLong(channelBuffer), new Bit(channelBuffer, 0));
    }

    public BasicAck(UnsignedLongLong deliveryTag, Bit multiple) {
        this.deliveryTag = deliveryTag;
        this.multiple = multiple;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
