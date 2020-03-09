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
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method requests a specific quality of service. The QoS can
 * be specified for the current channel or for all channels on the
 * connection. The particular properties and semantics of a qos method
 * always depend on the content class semantics. Though the qos method
 * could in principle apply to both peers, it is currently meaningful
 * only for the server.
 */
@Getter
public final class BasicQos extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(10);
    protected static final String METHOD_NAME = "qos";

    private UnsignedLong prefetchSize;
    private UnsignedShort prefetchCount;
    private Bit global;

    public BasicQos(ByteBuf channelBuffer) {
        this(new UnsignedLong(channelBuffer), new UnsignedShort(channelBuffer), new Bit(channelBuffer, 0));
    }

    public BasicQos(UnsignedLong prefetchSize, UnsignedShort prefetchCount, Bit global) {
        this.prefetchSize = prefetchSize;
        this.prefetchCount = prefetchCount;
        this.global = global;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
