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

/**
 * This method attempts to authenticate, passing a block of SASL data
 * for the security mechanism at the server side.
 */

public final class ConnectionSecureOk extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(21);
    protected static final String METHOD_NAME = "secure-ok";

    private LongString response;

    public ConnectionSecureOk(ByteBuf channelBuffer) {
        this(new LongString(channelBuffer));
    }

    public ConnectionSecureOk(LongString response) {
        this.response = response;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public LongString getResponse() {
        return this.response;
    }
}
