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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpClientDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpEncoder;
import java.net.SocketAddress;
import java.util.concurrent.CountDownLatch;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

/**
 * Amqp Encode Handler Tests.
 */
@Log4j2
public class AmqpEncodeHandlerTest {

    private AmqpClientDecoder clientDecoder;

    private EventLoopGroup group;

    private MethodRegistry methodRegistry;

    private AmqpClientChannel clientChannel;

    @Before
    public void setup() {
        group = new NioEventLoopGroup();
        clientChannel = new AmqpClientChannel();
        clientDecoder = new AmqpClientDecoder(new AmqpClientMethodProcessor(clientChannel));
        methodRegistry = new MethodRegistry(ProtocolVersion.v0_91);

    }

    @After
    public void destroy() {
        group.shutdownGracefully();
    }

    private Channel newServer(final boolean autoRead, final ChannelHandler... handlers) {
        assertTrue(handlers.length >= 1);
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.childOption(ChannelOption.ALLOCATOR, PulsarByteBufAllocator.DEFAULT);
        serverBootstrap.childOption(ChannelOption.TCP_NODELAY, true);
        serverBootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
            new AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024));
        serverBootstrap.channel(EventLoopUtil.getServerSocketChannelClass(group));
        EventLoopUtil.enableTriggeredMode(serverBootstrap);
        serverBootstrap.group(group)
            .childOption(ChannelOption.AUTO_READ, autoRead)
            .childHandler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(handlers);
                }
            });
        return serverBootstrap.bind(0)
            .syncUninterruptibly()
            .channel();
    }

    private Channel newClient(SocketAddress server, final ChannelHandler... handlers) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
            .handler(new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(handlers);
                }
            });

        return bootstrap.connect(server)
            .syncUninterruptibly()
            .channel();
    }

    @Test
    public void testEncode() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        Channel server = newServer(true, new AmqpEncoder(), new ChannelInboundHandlerAdapter() {

            @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                ConnectionSecureBody cmd = methodRegistry.createConnectionSecureBody(new byte[0]);
                ctx.channel().writeAndFlush(cmd.generateFrame(1));
                super.channelRead(ctx, msg);
            }
        });
        Channel client = newClient(server.localAddress(), new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                ByteBuf buffer = (ByteBuf) msg;
                try {
                    clientDecoder.decodeBuffer(buffer.nioBuffer());
                    AMQBody response = (AMQBody) clientChannel.poll();
                    Assert.assertTrue(response instanceof ConnectionSecureBody);
                    latch.countDown();
                } catch (Exception e) {

                }

            }
        });
        try {

            client.writeAndFlush(newOneMessage())
                .syncUninterruptibly();
            // We received three messages even through auto reading
            // was turned off after we received the first message.
            assertTrue(latch.await(3000000L, SECONDS));

        } finally {
            client.close();
            server.close();
        }
    }

    private static ByteBuf newOneMessage() {
        return Unpooled.wrappedBuffer(new byte[] {1});
    }

}
