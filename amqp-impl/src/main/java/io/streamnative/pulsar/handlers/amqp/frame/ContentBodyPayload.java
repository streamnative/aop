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
package io.streamnative.pulsar.handlers.amqp.frame;

import static io.netty.buffer.ByteBufUtil.hexDump;
import static io.netty.buffer.Unpooled.wrappedBuffer;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.Octet;
import io.streamnative.pulsar.handlers.amqp.frame.types.Payload;

/**
 *  ContentBody frame (id 3).
 */
public class ContentBodyPayload implements Payload {

    public static final Octet FRAME_ID = new Octet(3);

    private final ByteBuf content;

    public ContentBodyPayload(ByteBuf channelBuffer) {
        this.content = wrappedBuffer(channelBuffer);
    }

    public ByteBuf getContent() {
        return content;
    }

    public Octet getFrameId() {
        return FRAME_ID;
    }

    public int getSize() {
        return content.readableBytes();
    }

    @Override
    public String toString() {
        return "ContentBodyPayload{" + "bodySize=" + content.readableBytes()
               + ", content=" + hexDump(content) + '}';
    }
}
