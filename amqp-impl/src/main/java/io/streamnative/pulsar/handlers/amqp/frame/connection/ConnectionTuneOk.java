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
package io.streamnative.pulsar.handlers.amqp.frame.connection;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method sends the client's connection tuning parameters to the
 * server. Certain fields are negotiated, others provide capability
 * information.
 */
@Getter
public final class ConnectionTuneOk extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(31);
    protected static final String METHOD_NAME = "tune-ok";

    private UnsignedShort channelMax;
    private UnsignedLong frameMax;
    private UnsignedShort heartbeat;

    public ConnectionTuneOk(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new UnsignedLong(channelBuffer),
                new UnsignedShort(channelBuffer));
    }

    public ConnectionTuneOk(UnsignedShort channelMax, UnsignedLong frameMax, UnsignedShort heartbeat) {
        this.channelMax = channelMax;
        this.frameMax = frameMax;
        this.heartbeat = heartbeat;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
