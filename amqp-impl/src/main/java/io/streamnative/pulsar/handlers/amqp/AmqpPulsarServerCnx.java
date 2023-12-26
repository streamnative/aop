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
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.ServerCnx;

/**
 * AmqpPulsarServerCnx extend ServerCnx.
 */
public class AmqpPulsarServerCnx extends ServerCnx {

    public AmqpPulsarServerCnx(PulsarService pulsar, ChannelHandlerContext ctx) {
        super(pulsar);
        this.ctx = ctx;
        this.remoteAddress = ctx.channel().remoteAddress();
    }

    @Override
    public void closeConsumer(Consumer consumer, Optional<BrokerLookupData> assignedBrokerLookupData) {
        // avoid close the connection when closing the consumer
    }
}
