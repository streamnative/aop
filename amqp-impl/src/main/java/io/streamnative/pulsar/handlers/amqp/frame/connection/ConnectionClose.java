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
 * This method indicates that the sender wants to close the connection.
 * This may be due to internal conditions (e.g. a forced shut-down) or
 * due to an error handling a specific method, i.e. an exception. When
 * a close is due to an exception, the sender provides the class and
 * method id of the method which caused the exception.
 */
@Getter
public final class ConnectionClose extends Connection {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(60);
    protected static final String METHOD_NAME = "close";

    private UnsignedShort replyCode;
    private ShortString replyText;
    private UnsignedShort classId;
    private UnsignedShort methodId;

    public ConnectionClose(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new UnsignedShort(channelBuffer),
                new UnsignedShort(channelBuffer));
    }

    public ConnectionClose(UnsignedShort replyCode,
                           ShortString replyText,
                           UnsignedShort classId,
                           UnsignedShort methodId) {
        this.replyCode = replyCode;
        this.replyText = replyText;
        this.classId = classId;
        this.methodId = methodId;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
