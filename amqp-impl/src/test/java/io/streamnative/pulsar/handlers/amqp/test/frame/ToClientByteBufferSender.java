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
package io.streamnative.pulsar.handlers.amqp.test.frame;

import io.streamnative.pulsar.handlers.amqp.AmqpByteBufferSender;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.mockito.Mockito;

/**
 * Sender for the server send byte buffer to the client.
 */
public class ToClientByteBufferSender extends AmqpByteBufferSender {

    private final ClientDecoder clientDecoder;

    public ToClientByteBufferSender(ClientDecoder clientDecoder) {
        super(Mockito.mock(AmqpConnection.class));
        this.clientDecoder = clientDecoder;
    }

    @Override
    public void flush() {
        try {
            clientDecoder.decodeBuffer(buf.nioBuffer());
        } catch (AMQFrameDecodingException e) {
            throw new RuntimeException(e);
        } finally {
            buf.clear();
        }
    }

    @Override
    public void close() {

    }
}
