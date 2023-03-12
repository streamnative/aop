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

import io.streamnative.pulsar.handlers.amqp.extension.ExchangeBindBody;
import io.streamnative.pulsar.handlers.amqp.extension.ExchangeUnbindBody;
import io.streamnative.pulsar.handlers.amqp.extension.ExtensionServerChannelMethodProcessor;
import java.io.IOException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQFrameDecodingException;
import org.apache.qpid.server.protocol.v0_8.ServerDecoder;
import org.apache.qpid.server.protocol.v0_8.transport.AMQProtocolVersionException;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicNackBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicQosBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRecoverSyncBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicRejectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelFlowOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionOpenBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueuePurgeBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

/**
 * Amqp broker decoder for amqp protocol requests decoding.
 */
public class AmqpBrokerDecoder extends ServerDecoder {

    private static final int bufferSize = 4 * 1024;
    private volatile QpidByteBuffer netInputBuffer;

    /**
     * Creates a new AMQP decoder.
     *
     * @param methodProcessor method processor
     */
    public AmqpBrokerDecoder(ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor) {
        super(methodProcessor);
        netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public ServerMethodProcessor<? extends ServerChannelMethodProcessor> getMethodProcessor() {
        return super.getMethodProcessor();
    }

    @Override
    public void decodeBuffer(QpidByteBuffer buf) throws AMQFrameDecodingException, AMQProtocolVersionException,
            IOException {
        int messageSize = buf.remaining();
        if (netInputBuffer.remaining() < messageSize) {
            QpidByteBuffer oldBuffer = netInputBuffer;
            if (oldBuffer.position() != 0) {
                oldBuffer.limit(oldBuffer.position());
                oldBuffer.slice();
                oldBuffer.flip();
                netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize + oldBuffer.remaining() + messageSize);
                netInputBuffer.put(oldBuffer);
            } else {
                netInputBuffer = QpidByteBuffer.allocateDirect(bufferSize + messageSize);
            }
        }
        netInputBuffer.put(buf);
        netInputBuffer.flip();
        super.decodeBuffer(netInputBuffer);
        restoreApplicationBufferForWrite();
    }

    protected void restoreApplicationBufferForWrite() {
        try (QpidByteBuffer oldNetInputBuffer = netInputBuffer) {
            int unprocessedDataLength = netInputBuffer.remaining();
            netInputBuffer.limit(netInputBuffer.capacity());
            netInputBuffer = oldNetInputBuffer.slice();
            netInputBuffer.limit(unprocessedDataLength);
        }
        if (netInputBuffer.limit() != netInputBuffer.capacity()) {
            netInputBuffer.position(netInputBuffer.limit());
            netInputBuffer.limit(netInputBuffer.capacity());
        } else {
            try (QpidByteBuffer currentBuffer = netInputBuffer) {
                int newBufSize;
                if (currentBuffer.capacity() < bufferSize) {
                    newBufSize = bufferSize;
                } else {
                    newBufSize = currentBuffer.capacity() + bufferSize;
                }
                netInputBuffer = QpidByteBuffer.allocateDirect(newBufSize);
                netInputBuffer.put(currentBuffer);
            }
        }
    }

    public void close() {
        netInputBuffer = null;
    }


    @Override
    protected void processMethod(int channelId, QpidByteBuffer in) throws AMQFrameDecodingException {
        ServerMethodProcessor<? extends ServerChannelMethodProcessor> methodProcessor = getMethodProcessor();
        final int classAndMethod = in.getInt();
        int classId = classAndMethod >> 16;
        int methodId = classAndMethod & 0xFFFF;
        methodProcessor.setCurrentMethod(classId, methodId);
        try {
            switch (classAndMethod) {
                //CONNECTION_CLASS:
                case 0x000a000b:
                    ConnectionStartOkBody.process(in, methodProcessor);
                    break;
                case 0x000a0015:
                    ConnectionSecureOkBody.process(in, methodProcessor);
                    break;
                case 0x000a001f:
                    ConnectionTuneOkBody.process(in, methodProcessor);
                    break;
                case 0x000a0028:
                    ConnectionOpenBody.process(in, methodProcessor);
                    break;
                case 0x000a0032:
                    if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8)) {
                        throw newUnknownMethodException(classId, methodId, methodProcessor.getProtocolVersion());
                    } else {
                        ConnectionCloseBody.process(in, methodProcessor);
                    }
                    break;
                case 0x000a0033:
                    if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8)) {
                        throw newUnknownMethodException(classId, methodId, methodProcessor.getProtocolVersion());
                    } else {
                        methodProcessor.receiveConnectionCloseOk();
                    }
                    break;
                case 0x000a003c:
                    if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8)) {
                        ConnectionCloseBody.process(in, methodProcessor);
                    } else {
                        throw newUnknownMethodException(classId, methodId, methodProcessor.getProtocolVersion());
                    }
                    break;
                case 0x000a003d:
                    if (methodProcessor.getProtocolVersion().equals(ProtocolVersion.v0_8)) {
                        methodProcessor.receiveConnectionCloseOk();
                    } else {
                        throw newUnknownMethodException(classId, methodId, methodProcessor.getProtocolVersion());
                    }
                    break;

                // CHANNEL_CLASS:

                case 0x0014000a:
                    ChannelOpenBody.process(channelId, in, methodProcessor);
                    break;
                case 0x00140014:
                    ChannelFlowBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00140015:
                    ChannelFlowOkBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00140028:
                    ChannelCloseBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00140029:
                    methodProcessor.getChannelMethodProcessor(channelId).receiveChannelCloseOk();
                    break;

                // ACCESS_CLASS:

                case 0x001e000a:
                    AccessRequestBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;

                // EXCHANGE_CLASS:

                case 0x0028000a:
                    ExchangeDeclareBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00280014:
                    ExchangeDeleteBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00280016:
                    ExchangeBoundBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x0028001e:
                    ExchangeBindBody.process(in, (ExtensionServerChannelMethodProcessor) methodProcessor
                            .getChannelMethodProcessor(channelId));
                    break;
                case 0x00280028:
                    ExchangeUnbindBody.process(in, (ExtensionServerChannelMethodProcessor) methodProcessor
                            .getChannelMethodProcessor(channelId));
                    break;

                // QUEUE_CLASS:

                case 0x0032000a:
                    QueueDeclareBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00320014:
                    QueueBindBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x0032001e:
                    QueuePurgeBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00320028:
                    QueueDeleteBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x00320032:
                    QueueUnbindBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;


                // BASIC_CLASS:

                case 0x003c000a:
                    BasicQosBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0014:
                    BasicConsumeBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c001e:
                    BasicCancelBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0028:
                    BasicPublishBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0046:
                    BasicGetBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0050:
                    BasicAckBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c005a:
                    BasicRejectBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0064:
                    BasicRecoverBody.process(in, methodProcessor.getProtocolVersion(),
                            methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0066:
                    BasicRecoverSyncBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c006e:
                    BasicRecoverSyncBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;
                case 0x003c0078:
                    BasicNackBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;

                // CONFIRM CLASS:

                case 0x0055000a:
                    ConfirmSelectBody.process(in, methodProcessor.getChannelMethodProcessor(channelId));
                    break;

                // TX_CLASS:

                case 0x005a000a:
                    if (!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk()) {
                        methodProcessor.getChannelMethodProcessor(channelId).receiveTxSelect();
                    }
                    break;
                case 0x005a0014:
                    if (!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk()) {
                        methodProcessor.getChannelMethodProcessor(channelId).receiveTxCommit();
                    }
                    break;
                case 0x005a001e:
                    if (!methodProcessor.getChannelMethodProcessor(channelId).ignoreAllButCloseOk()) {
                        methodProcessor.getChannelMethodProcessor(channelId).receiveTxRollback();
                    }
                    break;

                default:
                    throw newUnknownMethodException(classId, methodId, methodProcessor.getProtocolVersion());

            }
        } finally {
            methodProcessor.setCurrentMethod(0, 0);
        }
    }

}
