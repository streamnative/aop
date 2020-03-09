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
package io.streamnative.pulsar.handlers.amqp.frame.methods.exchange;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import lombok.Getter;

/**
 * This method deletes an exchange. When an exchange is deleted all queue
 * bindings on the exchange are cancelled.
 */
@Getter
public final class ExchangeDelete extends Exchange {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(20);
    protected static final String METHOD_NAME = "delete";

    private UnsignedShort ticket;
    private ShortString exchange;
    private Bit ifUnused;
    private Bit nowait;

    public ExchangeDelete(ByteBuf channelBuffer) {
        this(new UnsignedShort(channelBuffer),
                new ShortString(channelBuffer),
                new Bit(channelBuffer, 0),
                new Bit(channelBuffer, 1));
    }

    public ExchangeDelete(UnsignedShort ticket, ShortString exchange,
                          Bit ifUnused, Bit nowait) {
        this.ticket = ticket;
        this.exchange = exchange;
        this.ifUnused = ifUnused;
        this.nowait = nowait;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
