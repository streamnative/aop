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
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;

/**
 * A channel initializer that initialize channels for amqp protocol.
 */
public class AmqpChannelInitializer extends ChannelInitializer<SocketChannel> {

    public static final int MAX_FRAME_LENGTH = 100 * 1024 * 1024; // 100MB

    @Getter
    private final PulsarService pulsarService;
    @Getter
    private final AmqpServiceConfiguration amqpConfig;

    public AmqpChannelInitializer(PulsarService pulsarService,
                                   AmqpServiceConfiguration amqpConfig)
            throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.amqpConfig = amqpConfig;
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
        ch.pipeline().addLast("frameEncoder",
            new AmqpEncoder());
        ch.pipeline().addLast("handler",
            new AmqpConnection(pulsarService, amqpConfig));
    }

}
