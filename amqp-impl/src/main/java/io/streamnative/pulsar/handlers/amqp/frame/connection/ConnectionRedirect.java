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
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method redirects the client to another server, based on the
 * requested virtual host and/or capabilities.
 */
@Getter
public final class ConnectionRedirect extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(50);
    protected static final String METHOD_NAME = "redirect";

    private ShortString host;
    private ShortString knownHosts;

    public ConnectionRedirect(ByteBuf channelBuffer) {
        this(new ShortString(channelBuffer), new ShortString(channelBuffer));
    }

    public ConnectionRedirect(ShortString host, ShortString knownHosts) {
        this.host = host;
        this.knownHosts = knownHosts;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

}
