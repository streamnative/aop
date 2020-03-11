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
package io.streamnative.pulsar.handlers.amqp.example;

import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpServerMethodProcessor;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.transport.ByteBufferSender;

import java.util.Arrays;

public class AmqpBrokerDecoderExample {

    public static void main(String[] args) throws Exception {
        AmqpServerMethodProcessor serverMethodProcessor = new AmqpServerMethodProcessor(null);
        serverMethodProcessor.receiveChannelOpen(1);
        AmqpBrokerDecoder decoder = new AmqpBrokerDecoder(serverMethodProcessor);
        BasicGetBody basicGetBody = new BasicGetBody(1, AMQShortString.createAMQShortString("test"), false);
        decoder.setExpectProtocolInitiation(false);
        QpidByteBuffer buffer = QpidByteBuffer.allocateDirect(1024);

        AMQFrame frame = new AMQFrame(1, basicGetBody);
        frame.writePayload(new ByteBufferSender() {
            @Override
            public boolean isDirectBufferPreferred() {
                return false;
            }

            @Override
            public void send(QpidByteBuffer msg) {
                buffer.put(msg);
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }
        });
        decoder.decodeBuffer(buffer.flip());
    }
}
