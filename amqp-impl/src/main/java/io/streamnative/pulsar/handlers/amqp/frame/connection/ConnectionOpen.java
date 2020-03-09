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
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method opens a connection to a virtual host, which is a
 * collection of resources, and acts to separate multiple application
 * domains within a server.
 */
@Getter
public final class ConnectionOpen extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(40);
    protected static final String METHOD_NAME = "open";

    private ShortString virtualHost;
    private ShortString capabilities;
    private Bit insist;

    public ConnectionOpen(ByteBuf channelBuffer) {
        this(new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0));
    }

    public ConnectionOpen(ShortString virtualHost, ShortString capabilities, Bit insist) {
        this.virtualHost = virtualHost;
        this.capabilities = capabilities;
        this.insist = insist;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
