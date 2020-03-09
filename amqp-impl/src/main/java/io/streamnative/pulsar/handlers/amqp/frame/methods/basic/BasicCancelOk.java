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
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method confirms that the cancellation was completed.
 */
@Getter
public final class BasicCancelOk extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(31);
    protected static final String METHOD_NAME = "cancel-ok";

    private ShortString consumerTag;

    public BasicCancelOk(ByteBuf channelBuffer) {
        this(new ShortString(channelBuffer));
    }

    public BasicCancelOk(ShortString consumerTag) {
        this.consumerTag = consumerTag;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
