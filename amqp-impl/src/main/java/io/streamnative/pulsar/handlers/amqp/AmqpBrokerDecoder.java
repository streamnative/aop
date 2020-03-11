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

import org.apache.qpid.server.protocol.v0_8.ServerDecoder;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

/**
 * Amqp broker decoder for amqp protocol requests decoding.
 */
public class AmqpBrokerDecoder extends ServerDecoder {

    /**
     * Creates a new AMQP decoder.
     *
     * @param methodProcessor method processor
     */
    public AmqpBrokerDecoder(ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor) {
        super(methodProcessor);
    }

    @Override
    public ServerMethodProcessor<? extends ServerChannelMethodProcessor> getMethodProcessor() {
        return super.getMethodProcessor();
    }
}
