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

import java.nio.ByteBuffer;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.transport.ByteBufferSender;
/**
 * Frame assembly class.
 */
public class SimpleEncodeBufferSender implements ByteBufferSender {

    private ByteBuffer byteBuffer;

    private boolean isDirect;

    public SimpleEncodeBufferSender(int capacity, boolean isDirect) {
        this.isDirect = isDirect;
        byteBuffer = isDirect ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
    }

    @Override public boolean isDirectBufferPreferred() {
        return isDirect;
    }

    @Override public void send(QpidByteBuffer buffer) {
        byteBuffer.put(((SingleQpidByteBuffer) buffer.duplicate()).getUnderlyingBuffer());
    }

    @Override public void flush() {

    }

    @Override public void close() {

    }

    public ByteBuffer getBuffer() {
        return byteBuffer;
    }
}
