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

import java.io.IOException;
import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.message.MessageContentSource;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.ProtocolOutputConverterImpl;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQVersionAwareProtocolSession;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.apache.qpid.server.transport.ByteBufferSender;
import org.apache.qpid.server.util.GZIPUtils;

/**
 * Used to process command output.
 */
@Log4j2
public class AmqpOutputConverter {

    private static final int BASIC_CLASS_ID = 60;
    private final AmqpConnection connection;
    private static final AMQShortString GZIP_ENCODING = AMQShortString.valueOf(GZIPUtils.GZIP_CONTENT_ENCODING);

    public AmqpOutputConverter(AmqpConnection connection) {
        this.connection = connection;
    }

    public long writeDeliver(final AmqpMessageData message, int channelId,
        boolean isRedelivered, long deliveryTag,
        AMQShortString consumerTag) {

        AMQBody deliverBody = createEncodedDeliverBody(message, isRedelivered, deliveryTag, consumerTag);
        return writeMessageDelivery(message, channelId, deliverBody);
    }

    private long writeMessageDelivery(AmqpMessageData message, int channelId, AMQBody deliverBody) {
        return writeMessageDelivery(message, message.getContentHeaderBody(), channelId, deliverBody);
    }

    interface DisposableMessageContentSource extends MessageContentSource {
        void dispose();
    }

    private long writeMessageDelivery(AmqpMessageData message, ContentHeaderBody contentHeaderBody, int channelId,
        AMQBody deliverBody) {

        int bodySize = (int) message.getContentHeaderBody().getBodySize();
        boolean msgCompressed = isCompressed(contentHeaderBody);
        DisposableMessageContentSource modifiedContent = null;

        boolean compressionSupported = connection.isCompressionSupported();

        long length;
        if (msgCompressed
            && !compressionSupported
            && (modifiedContent = inflateIfPossible(message)) != null) {
            BasicContentHeaderProperties modifiedProps =
                new BasicContentHeaderProperties(contentHeaderBody.getProperties());
            modifiedProps.setEncoding((String) null);

            length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
        } else if (!msgCompressed
            && compressionSupported
            && contentHeaderBody.getProperties().getEncoding() == null
            && bodySize > connection.getMessageCompressionThreshold()
            && (modifiedContent = deflateIfPossible(message)) != null) {
            BasicContentHeaderProperties modifiedProps =
                new BasicContentHeaderProperties(contentHeaderBody.getProperties());
            modifiedProps.setEncoding(GZIP_ENCODING);

            length = writeMessageDeliveryModified(modifiedContent, channelId, deliverBody, modifiedProps);
        } else {
            writeMessageDeliveryUnchanged(new ModifiedContentSource(message.getContentBody().getPayload()),
                channelId, deliverBody, contentHeaderBody, bodySize);

            length = bodySize;
        }

        if (modifiedContent != null) {
            modifiedContent.dispose();
        }

        return length;
    }

    private DisposableMessageContentSource deflateIfPossible(AmqpMessageData message) {
        try (QpidByteBuffer contentBuffers = message.getContentBody().getPayload()) {
            return new ModifiedContentSource(QpidByteBuffer.deflate(contentBuffers));
        } catch (IOException e) {
            log.warn("Unable to compress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

    private DisposableMessageContentSource inflateIfPossible(AmqpMessageData message) {
        try (QpidByteBuffer contentBuffers = message.getContentBody().getPayload()) {
            return new ModifiedContentSource(QpidByteBuffer.inflate(contentBuffers));
        } catch (IOException e) {
            log.warn("Unable to decompress message payload for consumer with gzip, message will be sent as is", e);
            return null;
        }
    }

    private int writeMessageDeliveryModified(final MessageContentSource content, final int channelId,
        final AMQBody deliverBody,
        final BasicContentHeaderProperties modifiedProps) {
        final int bodySize = (int) content.getSize();
        ContentHeaderBody modifiedHeaderBody = new ContentHeaderBody(modifiedProps, bodySize);
        writeMessageDeliveryUnchanged(content, channelId, deliverBody, modifiedHeaderBody, bodySize);
        return bodySize;
    }

    private void writeMessageDeliveryUnchanged(MessageContentSource content,
        int channelId, AMQBody deliverBody, ContentHeaderBody contentHeaderBody,
        int bodySize) {
        if (bodySize == 0) {
            ProtocolOutputConverterImpl.SmallCompositeAMQBodyBlock compositeBlock =
                new ProtocolOutputConverterImpl.SmallCompositeAMQBodyBlock(channelId, deliverBody,
                contentHeaderBody);

            writeFrame(compositeBlock);
        } else {
            int maxFrameBodySize = (int) connection.getMaxFrameSize() - AMQFrame.getFrameOverhead();
            try (QpidByteBuffer contentByteBuffer = content.getContent()) {
                int contentChunkSize = bodySize > maxFrameBodySize ? maxFrameBodySize : bodySize;
                QpidByteBuffer chunk = contentByteBuffer.view(0, contentChunkSize);
                writeFrame(new CompositeAMQBodyBlock(channelId,
                    deliverBody,
                    contentHeaderBody,
                    new MessageContentSourceBody(chunk), connection.getAmqpConfig().isAmqpProxyV2Enable()));

                int writtenSize = contentChunkSize;
                while (writtenSize < bodySize) {
                    contentChunkSize =
                        (bodySize - writtenSize) > maxFrameBodySize ? maxFrameBodySize : bodySize - writtenSize;
                    QpidByteBuffer chunkElement = contentByteBuffer.view(writtenSize, contentChunkSize);
                    writtenSize += contentChunkSize;
                    writeFrame(new AMQFrame(channelId, new MessageContentSourceBody(chunkElement)));

                }
            }
        }
    }

    private boolean isCompressed(final ContentHeaderBody contentHeaderBody) {
        return GZIP_ENCODING.equals(contentHeaderBody.getProperties().getEncoding());
    }

    private static class MessageContentSourceBody implements AMQBody {
        public static final byte TYPE = 3;
        private final int length;
        private final QpidByteBuffer content;

        private MessageContentSourceBody(QpidByteBuffer content) {
            this.content = content;
            length = content.remaining();
        }

        @Override
        public byte getFrameType() {
            return TYPE;
        }

        @Override
        public int getSize() {
            return length;
        }

        @Override
        public long writePayload(final ByteBufferSender sender) {
            try {
                sender.send(content);
            } finally {
                content.close();
            }
            return length;
        }

        @Override
        public void handle(int channelId, AMQVersionAwareProtocolSession amqProtocolSession) throws QpidException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return "[" + getClass().getSimpleName() + ", length: " + length + "]";
        }

    }

    public long writeGetOk(final AmqpMessageData message, int channelId,
        boolean isRedelivered, long deliveryTag, int messageCount) {
        AMQBody deliver = createEncodedGetOkBody(message, isRedelivered, deliveryTag, messageCount);
        return writeMessageDelivery(message, channelId, deliver);
    }

    private AMQBody createEncodedDeliverBody(AmqpMessageData message,
        boolean isRedelivered,
        final long deliveryTag,
        final AMQShortString consumerTag) {

        final AMQShortString exchangeName;
        final AMQShortString routingKey;

        final MessagePublishInfo pb = message.getMessagePublishInfo();
        exchangeName = pb.getExchange();
        routingKey = pb.getRoutingKey();

        return new EncodedDeliveryBody(deliveryTag, routingKey,
            exchangeName, consumerTag, isRedelivered);
    }

    private class EncodedDeliveryBody implements AMQBody {
        private final long deliveryTag;
        private final AMQShortString routingKey;
        private final AMQShortString exchangeName;
        private final AMQShortString consumerTag;
        private final boolean isRedelivered;
        private AMQBody underlyingBody;

        private EncodedDeliveryBody(long deliveryTag, AMQShortString routingKey, AMQShortString exchangeName,
            AMQShortString consumerTag, boolean isRedelivered) {
            this.deliveryTag = deliveryTag;
            this.routingKey = routingKey;
            this.exchangeName = exchangeName;
            this.consumerTag = consumerTag;
            this.isRedelivered = isRedelivered;
        }

        public AMQBody createAMQBody() {
            return connection.getMethodRegistry().createBasicDeliverBody(consumerTag,
                deliveryTag,
                isRedelivered,
                exchangeName,
                routingKey);
        }

        @Override
        public byte getFrameType() {
            return AMQMethodBody.TYPE;
        }

        @Override
        public int getSize() {
            if (underlyingBody == null) {
                underlyingBody = createAMQBody();
            }
            return underlyingBody.getSize();
        }

        @Override
        public long writePayload(ByteBufferSender sender) {
            if (underlyingBody == null) {
                underlyingBody = createAMQBody();
            }
            return underlyingBody.writePayload(sender);
        }

        @Override
        public void handle(final int channelId, final AMQVersionAwareProtocolSession amqProtocolSession)
            throws QpidException {
            throw new QpidException("This block should never be dispatched!");
        }

        @Override
        public String toString() {
            return "[" + getClass().getSimpleName() + " underlyingBody: " + String.valueOf(underlyingBody) + "]";
        }
    }

    private AMQBody createEncodedGetOkBody(AmqpMessageData message,
        boolean isRedelivered,
        final long deliveryTag,
        int messageCount) {
        final AMQShortString exchangeName;
        final AMQShortString routingKey;

        final MessagePublishInfo pb = message.getMessagePublishInfo();
        exchangeName = pb.getExchange();
        routingKey = pb.getRoutingKey();

        return connection.getMethodRegistry().createBasicGetOkBody(deliveryTag,
            isRedelivered,
            exchangeName,
            routingKey,
            messageCount);
    }

    private AMQBody createEncodedReturnFrame(MessagePublishInfo messagePublishInfo,
        int replyCode,
        AMQShortString replyText) {

        return connection.getMethodRegistry().createBasicReturnBody(replyCode,
            replyText,
            messagePublishInfo.getExchange(),
            messagePublishInfo.getRoutingKey());
    }

    public void writeReturn(MessagePublishInfo messagePublishInfo, ContentHeaderBody header, AmqpMessageData message,
        int channelId, int replyCode, AMQShortString replyText) {

        AMQBody returnFrame = createEncodedReturnFrame(messagePublishInfo, replyCode, replyText);

        writeMessageDelivery(message, header, channelId, returnFrame);
    }

    public void writeFrame(AMQDataBlock block) {
        connection.writeFrame(block);
    }

    public void confirmConsumerAutoClose(int channelId, AMQShortString consumerTag) {

        BasicCancelOkBody basicCancelOkBody = connection.getMethodRegistry().createBasicCancelOkBody(consumerTag);
        writeFrame(basicCancelOkBody.generateFrame(channelId));

    }

    /**
     *  CompositeAMQBodyBlock .
     */
    public static final class CompositeAMQBodyBlock extends AMQDataBlock {
        public static final int OVERHEAD = 3 * AMQFrame.getFrameOverhead();

        private final AMQBody methodBody;
        private final AMQBody headerBody;
        private final AMQBody contentBody;
        private final int channel;
        private final boolean amqpProxyV2Enable;

        public CompositeAMQBodyBlock(int channel, AMQBody methodBody, AMQBody headerBody, AMQBody contentBody,
                                     boolean amqpProxyV2Enable) {
            this.channel = channel;
            this.methodBody = methodBody;
            this.headerBody = headerBody;
            this.contentBody = contentBody;
            this.amqpProxyV2Enable = amqpProxyV2Enable;
        }

        @Override
        public long getSize() {
            return (amqpProxyV2Enable ? 8 : 0) + OVERHEAD + (long) methodBody.getSize() + (long) headerBody.getSize()
                    + (long) contentBody.getSize();
        }

        @Override
        public long writePayload(final ByteBufferSender sender) {
            if (amqpProxyV2Enable) {
                // wrap the delivery message data with a special type 9 to skip data decode in proxy
                QpidByteBuffer buffer = QpidByteBuffer.allocate(7);
                buffer.put((byte) 9);
                buffer.putUnsignedShort(0);
                buffer.putUnsignedInt(getSize() - 8);
                buffer.flip();
                sender.send(buffer);
            }

            long size = (new AMQFrame(channel, methodBody)).writePayload(sender);

            size += (new AMQFrame(channel, headerBody)).writePayload(sender);

            size += (new AMQFrame(channel, contentBody)).writePayload(sender);

            if (amqpProxyV2Enable) {
                // wrap the delivery message data to skip data decode in proxy
                QpidByteBuffer endBuffer = QpidByteBuffer.allocate(1);
                endBuffer.put((byte) 0xCE);
                endBuffer.flip();
                sender.send(endBuffer);
                size += 8;
            }
            return size;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("[").append(getClass().getSimpleName())
                .append(" methodBody=").append(methodBody)
                .append(", headerBody=").append(headerBody)
                .append(", contentBody=").append(contentBody)
                .append(", channel=").append(channel).append("]");
            return builder.toString();
        }

    }

    /**
     * SmallCompositeAMQBodyBlock .
     */
    public static final class SmallCompositeAMQBodyBlock extends AMQDataBlock {
        public static final int OVERHEAD = 2 * AMQFrame.getFrameOverhead();

        private final AMQBody methodBody;
        private final AMQBody headerBody;
        private final int channel;

        public SmallCompositeAMQBodyBlock(int channel, AMQBody methodBody, AMQBody headerBody) {
            this.channel = channel;
            this.methodBody = methodBody;
            this.headerBody = headerBody;

        }

        @Override
        public long getSize() {
            return OVERHEAD + (long) methodBody.getSize() + (long) headerBody.getSize();
        }

        @Override
        public long writePayload(final ByteBufferSender sender) {
            long size = (new AMQFrame(channel, methodBody)).writePayload(sender);
            size += (new AMQFrame(channel, headerBody)).writePayload(sender);
            return size;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append(getClass().getSimpleName())
                .append("methodBody=").append(methodBody)
                .append(", headerBody=").append(headerBody)
                .append(", channel=").append(channel).append("]");
            return builder.toString();
        }
    }

    private static class ModifiedContentSource implements DisposableMessageContentSource {
        private final QpidByteBuffer buffer;
        private final int size;

        public ModifiedContentSource(final QpidByteBuffer buffer) {
            this.buffer = buffer;
            size = this.buffer.remaining();
        }

        @Override
        public void dispose() {
            buffer.dispose();
        }

        @Override
        public QpidByteBuffer getContent() {
            return getContent(0, (int) getSize());
        }

        @Override
        public QpidByteBuffer getContent(final int offset, int length) {
            return buffer.view(offset, length);
        }

        @Override
        public long getSize() {
            return size;
        }
    }
}
