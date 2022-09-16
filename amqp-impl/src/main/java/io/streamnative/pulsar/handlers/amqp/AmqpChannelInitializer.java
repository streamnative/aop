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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.flush.FlushConsolidationHandler;

/**
 * A channel initializer that initialize channels for amqp protocol.
 */
public class AmqpChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final AmqpServiceConfiguration amqpConfig;
    private final AmqpBrokerService amqpBrokerService;

    public AmqpChannelInitializer(AmqpServiceConfiguration amqpConfig, AmqpBrokerService amqpBrokerService) {
        super();
        this.amqpConfig = amqpConfig;
        this.amqpBrokerService = amqpBrokerService;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
//        ch.pipeline().addLast(new LengthFieldPrepender(4));
        //0      1         3         7                 size+7 size+8
        //+------+---------+---------+ +-------------+ +-----------+
        //| type | channel |    size | | payload     | | frame-end |
        //+------+---------+---------+ +-------------+ +-----------+
        // octet   short      long       'size' octets   octet
//        ch.pipeline().addLast("frameDecoder",
//            new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 3, 4, 1, 0));
        ch.pipeline().addLast("consolidation", new FlushConsolidationHandler(
                amqpConfig.getAmqpExplicitFlushAfterFlushes(), true));
        ch.pipeline().addLast("frameEncoder", new AmqpEncoder());
        ch.pipeline().addLast("handler", new AmqpConnection(amqpConfig, amqpBrokerService));
    }

}
