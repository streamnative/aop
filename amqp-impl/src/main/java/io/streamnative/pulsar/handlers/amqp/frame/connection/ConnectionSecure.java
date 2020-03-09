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
import io.streamnative.pulsar.handlers.amqp.frame.types.LongString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * The SASL protocol works by exchanging challenges and responses until
 * both peers have received sufficient information to authenticate each
 * other. This method challenges the client to provide more information.
 */
@Getter
public final class ConnectionSecure extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(20);
    protected static final String METHOD_NAME = "secure";

    private LongString challenge;

    public ConnectionSecure(ByteBuf channelBuffer) {
        this(new LongString(channelBuffer));
    }

    public ConnectionSecure(LongString challenge) {
        this.challenge = challenge;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
