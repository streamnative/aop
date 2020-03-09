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

/**
 * This method returns an undeliverable message that was published
 * with the "immediate" flag set, or an unroutable message published
 * with the "mandatory" flag set. The reply code and text provide
 * information about the reason that the message was undeliverable.
 */
public final class BasicReturn extends Basic {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(50);
    protected static final String METHOD_NAME = "return";

    private UnsignedShort replyCode;
    private ShortString replyText;
    private ShortString exchange;
    private ShortString routingKey;

    public BasicReturn(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer),
                new ShortString(channelBuffer));
    }

    public BasicReturn(UnsignedShort replyCode, ShortString replyText,
                       ShortString exchange, ShortString routingKey) {
        this.replyCode = replyCode;
        this.replyText = replyText;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public UnsignedShort getReplyCode() {
        return this.replyCode;
    }

    public ShortString getReplyText() {
        return this.replyText;
    }

    public ShortString getExchange() {
        return this.exchange;
    }

    public ShortString getRoutingKey() {
        return this.routingKey;
    }
}
