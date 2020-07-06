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
package io.streamnative.pulsar.handlers.amqp.test;

import io.streamnative.pulsar.handlers.amqp.AmqpByteBufferSenderImpl;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;

/**
 * Sender for the client send byte buffer to the server.
 */
public class ToServerByteBufferSender extends AmqpByteBufferSenderImpl {

    public ToServerByteBufferSender(AmqpConnection connection) {
        super(connection);
    }

    @Override
    public void internalFlush() throws Exception {
        connection.channelRead(connection.getCtx(), buf);
    }

    @Override
    public void close() {

    }
}
