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
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method confirms a Declare method and confirms the name of the
 * queue, essential for automatically-named queues.
 */
@Getter
public final class QueueDeclareOk extends Queue {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(11);
    protected static final String METHOD_NAME = "declare-ok";

    private ShortString queue;
    private UnsignedLong messageCount;
    private UnsignedLong consumerCount;

    public QueueDeclareOk(ByteBuf channelBuffer) {
        this(new ShortString(channelBuffer),
                new UnsignedLong(channelBuffer),
                new UnsignedLong(channelBuffer));
    }

    public QueueDeclareOk(ShortString queue,
                          UnsignedLong messageCount,
                          UnsignedLong consumerCount) {
        this.queue = queue;
        this.messageCount = messageCount;
        this.consumerCount = consumerCount;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
