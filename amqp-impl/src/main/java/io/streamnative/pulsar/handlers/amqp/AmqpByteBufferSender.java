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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;

/**
 * Base class of ByteBufferSender.
 */
public abstract class AmqpByteBufferSender implements ByteBufferSender {

    protected CompositeByteBuf buf = ByteBufAllocator.DEFAULT.compositeDirectBuffer(128);

    @Override
    public boolean isDirectBufferPreferred() {
        return true;
    }

    @Override
    public void send(QpidByteBuffer buffer) {
        buf.retain();
        ByteBuffer byteBuffer = ((SingleQpidByteBuffer) buffer.duplicate()).getUnderlyingBuffer();
        buf.addComponent(true, Unpooled.wrappedBuffer(byteBuffer));
    }

    @Override
    public final void flush() {
        try {
            internalFlush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            buf.removeComponents(0, buf.numComponents());
            buf.clear();
        }
    }

    protected abstract void internalFlush() throws Exception;

    @Override
    public void close() {

    }
}
