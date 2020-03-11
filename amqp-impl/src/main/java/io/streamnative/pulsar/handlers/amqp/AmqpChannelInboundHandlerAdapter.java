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
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkState;

/**
 * Netty channel inbound handler adapter for Amqp protocol.
 */
@Slf4j
public class AmqpChannelInboundHandlerAdapter extends ChannelInboundHandlerAdapter {

    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);
    protected final PulsarService pulsarService;
    protected AmqpBrokerDecoder decoder;
    protected final AmqpServiceConfiguration amqpConfig;

    public AmqpChannelInboundHandlerAdapter(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsarService;
        this.decoder = new AmqpBrokerDecoder(new AmqpServerMethodProcessor(this));
        this.amqpConfig = amqpConfig;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        close();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Channel nettyChannel = ctx.channel();
        checkState(nettyChannel.equals(this.getCtx().channel()));
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;
        decoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
    }

    protected void close() {
        if (isActive.getAndSet(false)) {
            log.info("close netty channel {}", ctx.channel());
            ctx.close();
        }
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public AmqpBrokerDecoder getDecoder() {
        return decoder;
    }

    @VisibleForTesting
    void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    @VisibleForTesting
    void setDecoder(AmqpBrokerDecoder decoder) {
        this.decoder = decoder;
    }
}
