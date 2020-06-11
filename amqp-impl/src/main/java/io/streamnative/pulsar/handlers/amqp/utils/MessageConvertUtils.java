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
package io.streamnative.pulsar.handlers.amqp.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageData;
import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;

/**
 * Util for convert message between Pulsar and AMQP.
 */
@UtilityClass
@Slf4j
public final class MessageConvertUtils {

    private static final String FAKE_AMQP_PRODUCER_NAME = "fake_amqp_producer_name";
    private static final String DEFAULT_CHARSET_NAME = "ISO8859-1";
    private static final String PROP_DELIMITER = ".";
    private static final String BASIC_PROP_PRE = "_bp_" + PROP_DELIMITER;
    private static final String BASIC_PROP_HEADER_PRE = "_bph_" + PROP_DELIMITER;
    private static final String BASIC_PUBLISH_INFO_PRE = "_pi_" + PROP_DELIMITER;

    private static final String PROP_CONTENT_TYPE = BASIC_PROP_PRE + "content_type";
    private static final String PROP_ENCODING = BASIC_PROP_PRE + "encoding";
    private static final String PROP_DELIVERY_MODE = BASIC_PROP_PRE + "delivery_mode";
    private static final String PROP_PRIORITY_PRIORITY = BASIC_PROP_PRE + "priority";
    private static final String PROP_CORRELATION_ID = BASIC_PROP_PRE + "correlation_id";
    private static final String PROP_REPLY_TO = BASIC_PROP_PRE + "reply_to";
    private static final String PROP_EXPIRATION = BASIC_PROP_PRE + "expiration";
    private static final String PROP_MESSAGE_ID = BASIC_PROP_PRE + "message_id";
    private static final String PROP_TIMESTAMP = BASIC_PROP_PRE + "timestamp";
    private static final String PROP_TYPE = BASIC_PROP_PRE + "type";
    private static final String PROP_USER_ID = BASIC_PROP_PRE + "user_id";
    private static final String PROP_APP_ID = BASIC_PROP_PRE + "app_id";
    private static final String PROP_CLUSTER_ID = BASIC_PROP_PRE + "cluster_id";
    private static final String PROP_PROPERTY_FLAGS = BASIC_PROP_PRE + "property_flags";

    private static final String PROP_EXCHANGE = BASIC_PUBLISH_INFO_PRE + "exchange";
    private static final String PROP_IMMEDIATE = BASIC_PUBLISH_INFO_PRE + "immediate";
    private static final String PROP_MANDATORY = BASIC_PUBLISH_INFO_PRE + "mandatory";
    public static final String PROP_ROUTING_KEY = BASIC_PUBLISH_INFO_PRE + "routingKey";

    private static final Clock clock = Clock.systemDefaultZone();

    // convert qpid IncomingMessage to Pulsar MessageImpl
    public static MessageImpl<byte[]> toPulsarMessage(IncomingMessage incomingMessage)
            throws UnsupportedEncodingException {
        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);

        // value
        if (incomingMessage.getBodyCount() > 0) {
            ByteBuf byteBuf = Unpooled.buffer();
            for (int i = 0; i < incomingMessage.getBodyCount(); i++) {
                byteBuf.writeBytes(
                        ((SingleQpidByteBuffer) incomingMessage.getContentChunk(i).getPayload())
                                .getUnderlyingBuffer());
            }
            byte[] bytes = new byte[byteBuf.writerIndex()];
            byteBuf.readBytes(bytes, 0, byteBuf.writerIndex());
            builder.value(bytes);
        } else {
            builder.value(new byte[0]);
        }

        // basic properties
        ContentHeaderBody contentHeaderBody = incomingMessage.getContentHeader();
        BasicContentHeaderProperties props = contentHeaderBody.getProperties();
        if (props != null) {
            if (props.getTimestamp() > 0) {
                builder.eventTime(props.getTimestamp());
            }

            setProp(builder, PROP_CONTENT_TYPE, props.getContentTypeAsString());
            setProp(builder, PROP_ENCODING, props.getEncodingAsString());
            setProp(builder, PROP_DELIVERY_MODE, props.getDeliveryMode());
            setProp(builder, PROP_PRIORITY_PRIORITY, props.getPriority());
            setProp(builder, PROP_CORRELATION_ID, props.getCorrelationIdAsString());
            setProp(builder, PROP_REPLY_TO, props.getReplyToAsString());
            setProp(builder, PROP_EXPIRATION, props.getExpiration());
            setProp(builder, PROP_MESSAGE_ID, props.getMessageIdAsString());
            setProp(builder, PROP_TIMESTAMP, props.getTimestamp());
            setProp(builder, PROP_TYPE, props.getTypeAsString());
            setProp(builder, PROP_USER_ID, props.getUserIdAsString());
            setProp(builder, PROP_APP_ID, props.getAppIdAsString());
            setProp(builder, PROP_CLUSTER_ID, props.getClusterIdAsString());
            setProp(builder, PROP_PROPERTY_FLAGS, props.getPropertyFlags());

            Map<String, Object> headers = props.getHeadersAsMap();
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                setProp(builder, BASIC_PROP_HEADER_PRE + entry.getKey(), entry.getValue());
            }
        }

        setProp(builder, PROP_EXCHANGE, incomingMessage.getMessagePublishInfo().getExchange());
        setProp(builder, PROP_IMMEDIATE, incomingMessage.getMessagePublishInfo().isImmediate());
        setProp(builder, PROP_MANDATORY, incomingMessage.getMessagePublishInfo().isMandatory());
        setProp(builder, PROP_ROUTING_KEY, incomingMessage.getMessagePublishInfo().getRoutingKey());

        return (MessageImpl<byte[]>) builder.getMessage();
    }

    private void setProp(TypedMessageBuilder builder, String propName, Object value)
            throws UnsupportedEncodingException {
        if (value != null) {
            if (value instanceof Byte) {
                builder.property(propName, byteToString((byte) value));
            } else {
                builder.property(propName, String.valueOf(value));
            }
        }
    }

    // convert message to ByteBuf payload for ledger.addEntry.
    // parameter message is converted from passed in Kafka record.
    // called when publish received Kafka Record into Pulsar.
    public static ByteBuf messageToByteBuf(Message<byte[]> message) {
        checkArgument(message instanceof MessageImpl);

        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        PulsarApi.MessageMetadata.Builder msgMetadataBuilder = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadataBuilder.hasSequenceId()) {
            msgMetadataBuilder.setSequenceId(-1);
        }
        if (!msgMetadataBuilder.hasPublishTime()) {
            msgMetadataBuilder.setPublishTime(clock.millis());
        }
        if (!msgMetadataBuilder.hasProducerName()) {
            msgMetadataBuilder.setProducerName(FAKE_AMQP_PRODUCER_NAME);
        }

        msgMetadataBuilder.setCompression(
                CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadataBuilder.setUncompressedSize(payload.readableBytes());
        PulsarApi.MessageMetadata msgMetadata = msgMetadataBuilder.build();

        ByteBuf buf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, msgMetadata, payload);

        msgMetadataBuilder.recycle();
        msgMetadata.recycle();

        return buf;
    }

    public static Pair<BasicContentHeaderProperties, MessagePublishInfo> getPropertiesFromMetadata(
                                List<PulsarApi.KeyValue> propertiesList) throws UnsupportedEncodingException {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        Map<String, Object> headers = new HashMap<>();
        MessagePublishInfo messagePublishInfo = new MessagePublishInfo();

        for (PulsarApi.KeyValue keyValue : propertiesList) {
            switch (keyValue.getKey()) {
                case PROP_CONTENT_TYPE:
                    props.setContentType(keyValue.getValue());
                    break;
                case PROP_ENCODING:
                    props.setEncoding(keyValue.getValue());
                    break;
                case PROP_DELIVERY_MODE:
                    props.setDeliveryMode(stringToByte(keyValue.getValue()));
                    break;
                case PROP_PRIORITY_PRIORITY:
                    props.setPriority(stringToByte(keyValue.getValue()));
                    break;
                case PROP_CORRELATION_ID:
                    props.setCorrelationId(keyValue.getValue());
                    break;
                case PROP_REPLY_TO:
                    props.setReplyTo(keyValue.getValue());
                    break;
                case PROP_EXPIRATION:
                    props.setExpiration(Long.parseLong(keyValue.getValue()));
                    break;
                case PROP_MESSAGE_ID:
                    props.setMessageId(keyValue.getValue());
                    break;
                case PROP_TIMESTAMP:
                    props.setTimestamp(Long.parseLong(keyValue.getValue()));
                    break;
                case PROP_TYPE:
                    props.setType(keyValue.getValue());
                    break;
                case PROP_USER_ID:
                    props.setUserId(keyValue.getValue());
                    break;
                case PROP_APP_ID:
                    props.setAppId(keyValue.getValue());
                    break;
                case PROP_CLUSTER_ID:
                    props.setClusterId(keyValue.getValue());
                    break;
                case PROP_PROPERTY_FLAGS:
                    props.setPropertyFlags(Integer.parseInt(keyValue.getValue()));
                    break;
                case PROP_EXCHANGE:
                    messagePublishInfo.setExchange(AMQShortString.createAMQShortString(keyValue.getValue()));
                    break;
                case PROP_IMMEDIATE:
                    messagePublishInfo.setImmediate(Boolean.parseBoolean(keyValue.getValue()));
                    break;
                case PROP_MANDATORY:
                    messagePublishInfo.setMandatory(Boolean.parseBoolean(keyValue.getValue()));
                    break;
                case PROP_ROUTING_KEY:
                    messagePublishInfo.setRoutingKey(AMQShortString.createAMQShortString(keyValue.getValue()));
                    break;
                default:
                    headers.put(keyValue.getKey().substring(BASIC_PROP_HEADER_PRE.length()), keyValue.getValue());
            }
        }
        props.setHeaders(FieldTableFactory.createFieldTable(headers));
        return Pair.of(props, messagePublishInfo);
    }

    public static List<AmqpMessageData> entriesToAmqpBodyList(List<Entry> entries)
                                                                throws UnsupportedEncodingException {
        ImmutableList.Builder<AmqpMessageData> builder = ImmutableList.builder();
        // TODO convert bk entries to amqpbody,
        //  then assemble deliver body with ContentHeaderBody and ContentBody
        for (Entry entry : entries) {
            // each entry is a batched message
            ByteBuf metadataAndPayload = entry.getDataBuffer();
            PulsarApi.MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
            int numMessages = msgMetadata.getNumMessagesInBatch();
            boolean notBatchMessage = (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch());
            ByteBuf payload = metadataAndPayload.retain();

            if (log.isDebugEnabled()) {
                log.debug("entriesToRecords.  NumMessagesInBatch: {}, isBatchMessage: {}, entries in list: {}."
                                + " new entryId {}:{}, readerIndex: {},  writerIndex: {}",
                        numMessages, !notBatchMessage, entries.size(), entry.getLedgerId(),
                        entry.getEntryId(), payload.readerIndex(), payload.writerIndex());
            }

            // need handle encryption
            checkState(msgMetadata.getEncryptionKeysCount() == 0);

            if (notBatchMessage) {
                Pair<BasicContentHeaderProperties, MessagePublishInfo> metaData =
                        getPropertiesFromMetadata(msgMetadata.getPropertiesList());

                byte[] data = new byte[payload.readableBytes()];
                payload.readBytes(data);
                builder.add(AmqpMessageData.builder()
                        .messagePublishInfo(metaData.getRight())
                        .contentHeaderBody(new ContentHeaderBody(metaData.getLeft()))
                        .contentBody(new ContentBody(QpidByteBuffer.wrap(data)))
                        .build());
            } else {
                // currently, no consider for batch
            }
        }
        return builder.build();
    }

    public static AmqpMessageData entryToAmqpBody(Entry entry)
        throws UnsupportedEncodingException {
        AmqpMessageData amqpMessage = null;
        // TODO convert bk entries to amqpbody,
        //  then assemble deliver body with ContentHeaderBody and ContentBody

        ByteBuf metadataAndPayload = entry.getDataBuffer();
        PulsarApi.MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
        int numMessages = msgMetadata.getNumMessagesInBatch();
        boolean notBatchMessage = (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch());
        ByteBuf payload = metadataAndPayload.retain();

        if (log.isDebugEnabled()) {
            log.debug("entryToRecord.  NumMessagesInBatch: {}, isBatchMessage: {}."
                    + " new entryId {}:{}, readerIndex: {},  writerIndex: {}",
                numMessages, !notBatchMessage, entry.getLedgerId(),
                entry.getEntryId(), payload.readerIndex(), payload.writerIndex());
        }

        // need handle encryption
        checkState(msgMetadata.getEncryptionKeysCount() == 0);

        if (notBatchMessage) {
            Pair<BasicContentHeaderProperties, MessagePublishInfo> metaData =
                getPropertiesFromMetadata(msgMetadata.getPropertiesList());

            ContentHeaderBody contentHeaderBody = new ContentHeaderBody(metaData.getLeft());
            contentHeaderBody.setBodySize(payload.readableBytes());

            byte[] data = new byte[payload.readableBytes()];
            payload.readBytes(data);
            amqpMessage = AmqpMessageData.builder()
                .messagePublishInfo(metaData.getRight())
                .contentHeaderBody(contentHeaderBody)
                .contentBody(new ContentBody(QpidByteBuffer.wrap(data)))
                .build();
        } else {
            // currently, no consider for batch
        }

        return amqpMessage;
    }

    private static String byteToString(byte b) throws UnsupportedEncodingException {
        byte[] bytes = {b};
        return new String(bytes, DEFAULT_CHARSET_NAME);
    }

    private static Byte stringToByte(String string) throws UnsupportedEncodingException {
        if (string != null && string.length() > 0) {
            return string.getBytes(DEFAULT_CHARSET_NAME)[0];
        }
        return null;
    }

    // Currently, one entry consist of one IndexMessage info
    public static MessageImpl<byte[]> toPulsarMessage(IndexMessage indexMessage) {
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);
        builder.value(indexMessage.encode());
        return (MessageImpl<byte[]>) builder.getMessage();
    }

    // Currently, one entry consist of one IndexMessage info
    public static IndexMessage entryToIndexMessage(Entry entry) {
        ByteBuf metadataAndPayload = entry.getDataBuffer();
        Commands.parseMessageMetadata(metadataAndPayload);
        ByteBuf payload = metadataAndPayload.retain();

        long ledgerId = payload.readLong();
        long entryId = payload.readLong();
        String exchangeName = payload.readCharSequence(payload.readableBytes(), StandardCharsets.ISO_8859_1).toString();
        return IndexMessage.create(exchangeName, ledgerId, entryId);
    }

    public static Map<String, Object> getHeaders(Message<byte[]> message) {
        Map<String, Object> headers = new HashMap<>();

        for (Map.Entry<String, String> entry : message.getProperties().entrySet()) {
            if (entry.getKey().startsWith(BASIC_PROP_HEADER_PRE)) {
                headers.put(entry.getKey().replaceFirst(BASIC_PROP_HEADER_PRE, ""), entry.getValue());
            }
        }
        return headers;
    }

}
