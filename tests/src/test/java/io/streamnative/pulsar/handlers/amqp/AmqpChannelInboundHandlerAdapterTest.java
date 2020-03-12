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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.pulsar.broker.PulsarService;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.EncodableAMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test for AmqpChannelInboundHandlerAdapter
 */
public class AmqpChannelInboundHandlerAdapterTest {

    private AmqpChannelInboundHandlerAdapter amqpChannelInboundHandlerAdapter;
    private ChannelHandlerContext ctx;
    private ResponseProcessor responseProcessor;


    @BeforeClass
    public void setup() {
        amqpChannelInboundHandlerAdapter = new AmqpChannelInboundHandlerAdapter(Mockito.mock(PulsarService.class),
                Mockito.mock(AmqpServiceConfiguration.class));
        this.responseProcessor = new ResponseProcessor();
        ctx = Mockito.mock(ChannelHandlerContext.class);
        Mockito.when(ctx.channel()).thenReturn(Mockito.mock(Channel.class));
        amqpChannelInboundHandlerAdapter.setCtx(ctx);
        AmqpBrokerDecoder decoder = new AmqpBrokerDecoder(new MockServerMethodProcessor(
                amqpChannelInboundHandlerAdapter, responseProcessor));
        decoder.setExpectProtocolInitiation(false);
        amqpChannelInboundHandlerAdapter.setDecoder(decoder);
    }

    @Test
    public void testBasicGet() throws Exception {
        BasicGetBody basicGetBody = new BasicGetBody(1, AMQShortString.createAMQShortString("test"), false);
        QpidByteBuffer buffer = QpidByteBuffer.allocate(1024);
        AMQFrame frame = new AMQFrame(1, basicGetBody);
        frame.writePayload(new TestByteBufferSender(buffer));

        //todo QpidByteBuffer does not expose ability to get nio buffer, so it's better to implement
        // a ByteBuf based QpidByteBuffer
        buffer.flip();
        amqpChannelInboundHandlerAdapter.channelRead(ctx, Unpooled.wrappedBuffer(buffer.array(), 0, buffer.limit()));
        EncodableAMQDataBlock response = responseProcessor.getResponse();
        Assert.assertNotNull(response);
        Assert.assertTrue(response instanceof BasicGetOkBody);
    }

    private static class MockServerMethodProcessor extends AmqpServerMethodProcessor {

        private MockServerChannelMethodProcessor channelMethodProcessor;

        public MockServerMethodProcessor(AmqpChannelInboundHandlerAdapter inboundHandlerAdapter,
                                         ResponseProcessor responseProcessor) {
            super(inboundHandlerAdapter);
            this.channelMethodProcessor = new MockServerChannelMethodProcessor(this, responseProcessor);
        }

        @Override
        public void receiveChannelOpen(int channelId) {
            super.receiveChannelOpen(channelId);
        }

        @Override
        public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
            return channelMethodProcessor;
        }
    }

    private static class ResponseProcessor {

        private BlockingQueue<EncodableAMQDataBlock> responseQueue;

        public ResponseProcessor() {
            responseQueue = new ArrayBlockingQueue<>(1);
        }

        public void addResponse(EncodableAMQDataBlock response) {
            responseQueue.add(response);
        }

        public EncodableAMQDataBlock getResponse() {
            return responseQueue.poll();
        }

    }

    private static class MockServerChannelMethodProcessor extends AmqpServerChannelMethodProcessor {

        private final ResponseProcessor responseProcessor;

        public MockServerChannelMethodProcessor(AmqpServerMethodProcessor serverMethodProcessor,
                                                ResponseProcessor responseProcessor) {
            super(serverMethodProcessor);
            this.responseProcessor = responseProcessor;
        }

        @Override
        public void receiveBasicGet(AMQShortString queue, boolean noAck) {
            responseProcessor.addResponse(Mockito.mock(BasicGetOkBody.class));
        }
    }

    private static class TestByteBufferSender implements ByteBufferSender {

        private final QpidByteBuffer buffer;

        private TestByteBufferSender(QpidByteBuffer buffer) {
            this.buffer = buffer;
        }

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
    }
}
