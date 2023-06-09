/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import io.streamnative.pulsar.handlers.amqp.admin.model.PublishParams;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
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
    public static final String BASIC_PROP_PRE = "_bp_" + PROP_DELIMITER;
    public static final String BASIC_PROP_HEADER_PRE = "_bph_" + PROP_DELIMITER;
    private static final String BASIC_PUBLISH_INFO_PRE = "_pi_" + PROP_DELIMITER;

    public static final String PROP_CONTENT_TYPE = BASIC_PROP_PRE + "content_type";
    public static final String PROP_ENCODING = BASIC_PROP_PRE + "encoding";
    private static final String PROP_DELIVERY_MODE = BASIC_PROP_PRE + "delivery_mode";
    public static final String PROP_PRIORITY_PRIORITY = BASIC_PROP_PRE + "priority";
    private static final String PROP_CORRELATION_ID = BASIC_PROP_PRE + "correlation_id";
    private static final String PROP_REPLY_TO = BASIC_PROP_PRE + "reply_to";
    public static final String PROP_EXPIRATION = BASIC_PROP_PRE + "expiration";
    private static final String PROP_MESSAGE_ID = BASIC_PROP_PRE + "message_id";
    private static final String PROP_TIMESTAMP = BASIC_PROP_PRE + "timestamp";
    private static final String PROP_TYPE = BASIC_PROP_PRE + "type";
    private static final String PROP_USER_ID = BASIC_PROP_PRE + "user_id";
    private static final String PROP_APP_ID = BASIC_PROP_PRE + "app_id";
    private static final String PROP_CLUSTER_ID = BASIC_PROP_PRE + "cluster_id";
    private static final String PROP_PROPERTY_FLAGS = BASIC_PROP_PRE + "property_flags";

    public static final String PROP_EXCHANGE = BASIC_PUBLISH_INFO_PRE + "exchange";
    private static final String PROP_IMMEDIATE = BASIC_PUBLISH_INFO_PRE + "immediate";
    private static final String PROP_MANDATORY = BASIC_PUBLISH_INFO_PRE + "mandatory";
    public static final String PROP_ROUTING_KEY = BASIC_PUBLISH_INFO_PRE + "routingKey";
    public static final String BASIC_PROP_HEADER_X_DELAY = BASIC_PROP_HEADER_PRE + "x-delay";

    private static final Clock clock = Clock.systemDefaultZone();

    // convert qpid IncomingMessage to Pulsar MessageImpl
    public static MessageImpl<byte[]> toPulsarMessage(IncomingMessage incomingMessage)
            throws UnsupportedEncodingException {
        MessageImpl<byte[]> message;
        // value
        if (incomingMessage.getBodyCount() > 0) {
            ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer((int)incomingMessage.getContentHeader().getBodySize());
            for (int i = 0; i < incomingMessage.getBodyCount(); i++) {
                SingleQpidByteBuffer payload = (SingleQpidByteBuffer) incomingMessage.getContentChunk(i).getPayload();
                byteBuf.writeBytes(payload.getUnderlyingBuffer());
                payload.dispose();
            }
            message = MessageImpl.create(null, null, new MessageMetadata(), byteBuf,
                    Optional.empty(), null, Schema.BYTES, 0, true, -1L);
            byteBuf.release();
        } else {
            message = MessageImpl.create(null, null, new MessageMetadata(), Unpooled.EMPTY_BUFFER,
                    Optional.empty(), null, Schema.BYTES, 0, false, -1L);
        }
        MessageMetadata metadata = message.getMessageBuilder();
        // basic properties
        ContentHeaderBody contentHeaderBody = incomingMessage.getContentHeader();
        BasicContentHeaderProperties props = contentHeaderBody.getProperties();
        try {
            setProp(metadata, props);
            setProp(metadata, PROP_EXCHANGE, incomingMessage.getMessagePublishInfo().getExchange());
            setProp(metadata, PROP_IMMEDIATE, incomingMessage.getMessagePublishInfo().isImmediate());
            setProp(metadata, PROP_MANDATORY, incomingMessage.getMessagePublishInfo().isMandatory());
            setProp(metadata, PROP_ROUTING_KEY, incomingMessage.getMessagePublishInfo().getRoutingKey());
        } catch (UnsupportedEncodingException e) {
            message.release();
            throw e;
        }
        return message;
    }

    public static void setProp(TypedMessageBuilderImpl<byte[]> builder, BasicContentHeaderProperties props)
            throws UnsupportedEncodingException {
        if (props != null) {
            if (props.getTimestamp() > 0) {
                builder.eventTime(props.getTimestamp());
            }

            setProp(builder, PROP_CONTENT_TYPE, props.getContentTypeAsString());
            setProp(builder, PROP_ENCODING, props.getEncodingAsString());
            setProp(builder, PROP_DELIVERY_MODE, ((Byte) props.getDeliveryMode()).intValue());
            setProp(builder, PROP_PRIORITY_PRIORITY, ((Byte) props.getPriority()).intValue());
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
    }

    public static void setProp(MessageMetadata msgMetadata, BasicContentHeaderProperties props)
            throws UnsupportedEncodingException {
        if (props != null) {
            if (props.getTimestamp() > 0) {
                msgMetadata.setEventTime(props.getTimestamp());
            }

            setProp(msgMetadata, PROP_CONTENT_TYPE, props.getContentTypeAsString());
            setProp(msgMetadata, PROP_ENCODING, props.getEncodingAsString());
            setProp(msgMetadata, PROP_DELIVERY_MODE, ((Byte) props.getDeliveryMode()).intValue());
            setProp(msgMetadata, PROP_PRIORITY_PRIORITY, ((Byte) props.getPriority()).intValue());
            setProp(msgMetadata, PROP_CORRELATION_ID, props.getCorrelationIdAsString());
            setProp(msgMetadata, PROP_REPLY_TO, props.getReplyToAsString());
            setProp(msgMetadata, PROP_EXPIRATION, props.getExpiration());
            setProp(msgMetadata, PROP_MESSAGE_ID, props.getMessageIdAsString());
            setProp(msgMetadata, PROP_TIMESTAMP, props.getTimestamp());
            setProp(msgMetadata, PROP_TYPE, props.getTypeAsString());
            setProp(msgMetadata, PROP_USER_ID, props.getUserIdAsString());
            setProp(msgMetadata, PROP_APP_ID, props.getAppIdAsString());
            setProp(msgMetadata, PROP_CLUSTER_ID, props.getClusterIdAsString());
            setProp(msgMetadata, PROP_PROPERTY_FLAGS, props.getPropertyFlags());

            Map<String, Object> headers = props.getHeadersAsMap();
            for (Map.Entry<String, Object> entry : headers.entrySet()) {
                setProp(msgMetadata, BASIC_PROP_HEADER_PRE + entry.getKey(), entry.getValue());
            }
        }
    }

    public static MessageImpl<byte[]> toPulsarMessage(PublishParams params) {
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(null, Schema.BYTES);
        BasicContentHeaderProperties properties = new BasicContentHeaderProperties();
        properties.setDeliveryMode(Byte.parseByte(params.getDeliveryMode()));
        properties.setHeaders(FieldTable.convertToFieldTable(params.getHeaders()));
        properties.setContentType(params.getProps().get("content_type"));
        String expiration = params.getProps().get("expiration");
        if (NumberUtils.isCreatable(expiration)) {
            properties.setExpiration(Long.parseLong(expiration));
        }
        properties.setEncoding(params.getProps().get("content_encoding"));
        String priority = params.getProps().get("priority");
        if (NumberUtils.isCreatable(priority)) {
            properties.setPriority(Byte.parseByte(priority));
        }
        properties.setCorrelationId(params.getProps().get("correlation_id"));
        properties.setReplyTo(params.getProps().get("reply_to"));
        properties.setMessageId(params.getProps().get("message_id"));
        String timestamp = params.getProps().get("timestamp");
        if (NumberUtils.isCreatable(timestamp)) {
            properties.setTimestamp(Long.parseLong(timestamp));
        }
        properties.setType(params.getProps().get("type"));
        properties.setUserId(params.getProps().get("user_id"));
        properties.setAppId(params.getProps().get("app_id"));
        properties.setClusterId(params.getProps().get("cluster_id"));
        try {
            MessageConvertUtils.setProp(builder, properties);
            MessageConvertUtils.setProp(builder, MessageConvertUtils.PROP_EXCHANGE, params.getName());
            MessageConvertUtils.setProp(builder, MessageConvertUtils.PROP_ROUTING_KEY, params.getRoutingKey());
        } catch (UnsupportedEncodingException e) {
            throw new AoPServiceRuntimeException.NotSupportedOperationException(e.getMessage());
        }
        builder.value(params.getPayload().getBytes(StandardCharsets.UTF_8));
        return (MessageImpl<byte[]>) builder.getMessage();
    }

    public static void setProp(TypedMessageBuilder builder, String propName, Object value)
            throws UnsupportedEncodingException {
        if (value != null) {
            if (value instanceof Byte) {
                builder.property(propName, byteToString((byte) value));
            } else {
                builder.property(propName, String.valueOf(value));
            }
        }
    }

    public static void setProp(MessageMetadata metadata, String propName, Object value)
            throws UnsupportedEncodingException {
        if (value != null) {
            if (value instanceof Byte) {
                metadata.addProperty().setKey(propName).setValue(byteToString((byte) value));
            } else {
                metadata.addProperty().setKey(propName).setValue(String.valueOf(value));
            }
        }
    }

    // convert message to ByteBuf payload for ledger.addEntry.
    // parameter message is converted from passed in Kafka record.
    // called when publish received Kafka Record into Pulsar.
    public static ByteBuf messageToByteBuf(Message<byte[]> message) {
        checkArgument(message instanceof MessageImpl);

        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        MessageMetadata msgMetadata = msg.getMessageBuilder();
        ByteBuf payload = msg.getDataBuffer();

        // filled in required fields
        if (!msgMetadata.hasSequenceId()) {
            msgMetadata.setSequenceId(-1);
        }
        if (!msgMetadata.hasPublishTime()) {
            msgMetadata.setPublishTime(clock.millis());
        }
        if (!msgMetadata.hasProducerName()) {
            msgMetadata.setProducerName(FAKE_AMQP_PRODUCER_NAME);
        }

        msgMetadata.setCompression(
                CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        msgMetadata.setUncompressedSize(payload.readableBytes());

        ByteBuf buf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, msgMetadata, payload);
        msgMetadata.clear();
        return buf;
    }

    public static Pair<BasicContentHeaderProperties, MessagePublishInfo> getPropertiesFromMetadata(
            List<KeyValue> propertiesList) throws UnsupportedEncodingException {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        Map<String, Object> headers = new HashMap<>();
        MessagePublishInfo messagePublishInfo = new MessagePublishInfo();

        for (KeyValue keyValue : propertiesList) {
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

    public static Pair<BasicContentHeaderProperties, MessagePublishInfo> getPropertiesFromMetadata(
            Map<String, String> messageProperties) throws UnsupportedEncodingException {
        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        Map<String, Object> headers = new HashMap<>();
        MessagePublishInfo messagePublishInfo = new MessagePublishInfo();

        for (Map.Entry<String, String> keyValue : messageProperties.entrySet()) {
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
            MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
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
        MessageMetadata msgMetadata = Commands.parseMessageMetadata(metadataAndPayload);
        int numMessages = msgMetadata.getNumMessagesInBatch();
        boolean notBatchMessage = (numMessages == 1 && !msgMetadata.hasNumMessagesInBatch());
        ByteBuf payload = metadataAndPayload.retain();
        try {
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
        } finally {
            payload.release();
        }
    }

    public static AmqpMessageData messageToAmqpBody(Message<byte[]> message)
            throws UnsupportedEncodingException {
        AmqpMessageData amqpMessage;
        Map<String, String> messageProperties = new HashMap<>(message.getProperties());
        messageProperties.put(BASIC_PROP_HEADER_PRE + "pulsar_message_position", message.getMessageId().toString());
        Pair<BasicContentHeaderProperties, MessagePublishInfo> metaData =
                getPropertiesFromMetadata(messageProperties);

        ContentHeaderBody contentHeaderBody = new ContentHeaderBody(metaData.getLeft());
        MessageImpl<byte[]> msg = (MessageImpl<byte[]>) message;
        ByteBuf buf = msg.getDataBuffer();
        msg.getData();
        ByteBuffer byteBuffer;
        if (buf.isDirect()) {
            byteBuffer = ByteBuffer.allocateDirect(buf.readableBytes());
            buf.getBytes(buf.readerIndex(), byteBuffer);
        } else if (buf.arrayOffset() == 0 && buf.capacity() == buf.array().length) {
            byte[] array = buf.array();
            byteBuffer = ByteBuffer.wrap(array);
        } else {
            byteBuffer = ByteBuffer.allocateDirect(buf.readableBytes());
            buf.getBytes(buf.readerIndex(), byteBuffer);
        }
        byteBuffer.flip();
        contentHeaderBody.setBodySize(byteBuffer.limit());

        amqpMessage = AmqpMessageData.builder()
                .messagePublishInfo(metaData.getRight())
                .contentHeaderBody(contentHeaderBody)
                .contentBody(new ContentBody(QpidByteBuffer.wrap(byteBuffer)))
                .build();
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
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(null, Schema.BYTES);
        builder.value(indexMessage.encode());
        long deliverAtTime;
        if ((deliverAtTime = indexMessage.getDeliverAtTime()) > 0) {
            builder.deliverAfter(deliverAtTime, TimeUnit.MILLISECONDS);
        }
        return (MessageImpl<byte[]>) builder.getMessage();
    }

    // Currently, one entry consist of one IndexMessage info
    public static IndexMessage entryToIndexMessage(Entry entry) {
        ByteBuf metadataAndPayload = entry.getDataBuffer();
        Commands.parseMessageMetadata(metadataAndPayload);
        ByteBuf payload = metadataAndPayload.retain();

        try {
            long ledgerId = payload.readLong();
            long entryId = payload.readLong();
            String exchangeName = payload.readCharSequence(payload.readableBytes(), StandardCharsets.ISO_8859_1)
                    .toString();
            return IndexMessage.create(exchangeName, ledgerId, entryId, null);
        } finally {
            payload.release();
        }
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
