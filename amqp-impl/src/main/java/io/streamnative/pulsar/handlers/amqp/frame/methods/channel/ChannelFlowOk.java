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
package io.streamnative.pulsar.handlers.amqp.frame.methods.channel;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.Bit;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * Confirms to the peer that a flow command was received and processed.
 */
public final class ChannelFlowOk extends Channel {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(21);
    protected static final String METHOD_NAME = "flow-ok";

    private Bit active;

    public ChannelFlowOk(ByteBuf channelBuffer) {
        this(new Bit(channelBuffer, 0));
    }

    public ChannelFlowOk(Bit active) {
        this.active = active;
    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }

    public Bit getActive() {
        return this.active;
    }
}
