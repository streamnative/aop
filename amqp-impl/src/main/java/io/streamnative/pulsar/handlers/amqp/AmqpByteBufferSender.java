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

import io.netty.channel.ChannelHandlerContext;
import lombok.SneakyThrows;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;

/**
 * Use this sender to send byte buffer to client.
 */
public class AmqpByteBufferSender implements ByteBufferSender {

    private final ChannelHandlerContext ctx;

    public AmqpByteBufferSender(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public boolean isDirectBufferPreferred() {
        return true;
    }

    @SneakyThrows
    @Override
    public void send(QpidByteBuffer qpidByteBuffer) {
        QpidByteBuffer.write(ctx, qpidByteBuffer);
    }

    @Override
    public void flush() {
        ctx.flush();
    }

    @Override
    public void close() {

    }
}
