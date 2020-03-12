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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.transport.ByteBufferSender;

/**
 * Sender for the server send byte buffer to the client.
 */
public class ToClientByteBufferSender implements ByteBufferSender {

    private final QpidByteBuffer buffer = QpidByteBuffer.allocate(10 * 1024 * 1024);
    private final ClientDecoder clientDecoder;

    public ToClientByteBufferSender(ClientDecoder clientDecoder) {
        this.clientDecoder = clientDecoder;
    }

    @Override
    public boolean isDirectBufferPreferred() {
        return false;
    }

    @Override
    public void send(QpidByteBuffer qpidByteBuffer) {
        buffer.put(qpidByteBuffer.duplicate());
    }

    @Override
    public void flush() {
        buffer.flip();
        try {
            clientDecoder.decodeBuffer(((SingleQpidByteBuffer)buffer).getUnderlyingBuffer());
        } catch (AMQFrameDecodingException e) {
            throw new RuntimeException(e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public void close() {

    }
}
