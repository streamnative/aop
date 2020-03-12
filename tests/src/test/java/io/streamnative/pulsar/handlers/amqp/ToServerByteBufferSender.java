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

import io.netty.buffer.Unpooled;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;

/**
 * Sender for the client send byte buffer to the server.
 */
public class ToServerByteBufferSender implements ByteBufferSender {

    private final AmqpConnection connection;
    private final QpidByteBuffer buffer = QpidByteBuffer.allocate(10 * 1024 * 1024);

    public ToServerByteBufferSender(AmqpConnection connection) {
        this.connection = connection;
    }

    @Override
    public boolean isDirectBufferPreferred() {
        return false;
    }

    @Override
    public void send(QpidByteBuffer buffer) {
        this.buffer.put(buffer.duplicate());
    }

    @Override
    public void flush() {
        buffer.flip();
        try {
            connection.channelRead(connection.ctx, Unpooled.wrappedBuffer(((SingleQpidByteBuffer)buffer).getUnderlyingBuffer()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public void close() {

    }
}
