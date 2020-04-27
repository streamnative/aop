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
package io.streamnative.pulsar.handlers.amqp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import java.nio.ByteBuffer;
import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;

/**
 *  amqp output data encoder.
 */
@Log4j2
public class AmqpEncoder extends MessageToByteEncoder<AMQDataBlock> {

    @Override
    public void encode(ChannelHandlerContext ctx, AMQDataBlock frame, ByteBuf out) {
        try {

            SimpleEncodeBufferSender sender = new SimpleEncodeBufferSender((int) frame.getSize(), true);
            frame.writePayload(sender);
            ByteBuffer data = (ByteBuffer) sender.getBuffer().flip();
            out.writeBytes(data);
        } catch (Exception e) {
            log.error("channel {} encode exception {}", ctx.channel().toString(), e);
            ctx.close();
        }
    }
}
