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

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;


/**
 * A decoder that decodes amqp requests and responses.
 */
@Slf4j
public abstract class AmqpCommandDecoder extends ChannelInboundHandlerAdapter {
    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    private final PulsarService pulsarService;
    private final AmqpServiceConfiguration amqpConfig;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);
    protected AmqpBrokerDecoder brokerDecoder;

    protected AmqpCommandDecoder(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsarService;
        this.amqpConfig = amqpConfig;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }


    public AmqpBrokerDecoder getBrokerDecoder() {
        return brokerDecoder;
    }

    abstract void close();

    public PulsarService getPulsarService() {
        return pulsarService;
    }

    @VisibleForTesting
    public ChannelHandlerContext getCtx() {
        return ctx;
    }
}
