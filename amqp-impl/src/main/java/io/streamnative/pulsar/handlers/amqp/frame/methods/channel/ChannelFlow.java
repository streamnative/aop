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
 * This method asks the peer to pause or restart the flow of content
 * data. This is a simple flow-control mechanism that a peer can use
 * to avoid oveflowing its queues or otherwise finding itself receiving
 * more messages than it can process. Note that this method is not
 * intended for window control. The peer that receives a request to
 * stop sending content should finish sending the current content, if
 * any, and then wait until it receives a Flow restart method.
 */

public final class ChannelFlow extends Channel {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(20);
    protected static final String METHOD_NAME = "flow";

    private Bit active;

    public ChannelFlow(ByteBuf channelBuffer) {
        this(new Bit(channelBuffer, 0));
    }

    public ChannelFlow(Bit active) {
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
