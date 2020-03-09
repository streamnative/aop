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
import io.streamnative.pulsar.handlers.amqp.frame.types.FieldTable;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method selects a SASL security mechanism. ASL uses SASL
 * (RFC2222) to negotiate authentication and encryption.
 */
@Getter
public final class ConnectionStartOk extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(11);
    protected static final String METHOD_NAME = "start-ok";

    private FieldTable clientProperties;
    private ShortString mechanism;
    private FieldTable response;
    private ShortString locale;

    public ConnectionStartOk(ByteBuf channelBuffer) {
        this(new FieldTable(channelBuffer),
                new ShortString(channelBuffer),
                new FieldTable(channelBuffer),
                new ShortString(channelBuffer));
    }

    public ConnectionStartOk(FieldTable clientProperties,
                             ShortString mechanism,
                             FieldTable response,
                             ShortString locale) {
        this.clientProperties = clientProperties;
        this.mechanism = mechanism;
        this.response = response;
        this.locale = locale;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public FieldTable getClientProperties() {
        return this.clientProperties;
    }
}
