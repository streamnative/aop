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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import javafx.util.Pair;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.TypedMessageBuilderImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;


/**
 * Util for convert message between Pulsar and AMQP.
 */
@UtilityClass
@Slf4j
public final class MessageConvertUtils {

    private static final int DEFAULT_FETCH_BUFFER_SIZE = 1024 * 1024;
    private static final int MAX_RECORDS_BUFFER_SIZE = 100 * 1024 * 1024;
    private static final String FAKE_AMQP_PRODUCER_NAME = "fake_amqp_producer_name";

    private static final String PROP_DELIMITER = ".";
    private static final String BASIC_PROP_PRE = "_bp_" + PROP_DELIMITER;
    private static final String BASIC_PROP_HEADER_PRE = "_bph_" + PROP_DELIMITER;

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

    private static final Clock clock = Clock.systemDefaultZone();

    // convert qpid IncomingMessage to Pulsar MessageImpl
    public static MessageImpl<byte[]> toPulsarMessage(IncomingMessage incomingMessage) {
        @SuppressWarnings("unchecked")
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl(null, Schema.BYTES);

        // value
        if (incomingMessage.getBodyCount() > 0) {
            ByteBuf byteBuf = Unpooled.buffer(incomingMessage.getBodyCount());
            for (int i = 0; i < incomingMessage.getBodyCount(); i++) {
                byteBuf.writeBytes(incomingMessage.getContentChunk(i).getPayload().array());
            }
            builder.value(byteBuf.array());
        } else {
            builder.value(new byte[0]);
        }

        // basic properties
        ContentHeaderBody contentHeaderBody = incomingMessage.getContentHeader();
        BasicContentHeaderProperties basicContentHeaderProperties = contentHeaderBody.getProperties();
        if (basicContentHeaderProperties != null) {
            builder.eventTime(basicContentHeaderProperties.getTimestamp());

            builder.property(PROP_CONTENT_TYPE, basicContentHeaderProperties.getContentTypeAsString());
            builder.property(PROP_ENCODING, basicContentHeaderProperties.getEncodingAsString());
            builder.property(PROP_DELIVERY_MODE, String.valueOf(basicContentHeaderProperties.getDeliveryMode()));
            builder.property(PROP_PRIORITY_PRIORITY, String.valueOf(basicContentHeaderProperties.getPriority()));
            builder.property(PROP_CORRELATION_ID, basicContentHeaderProperties.getCorrelationIdAsString());
            builder.property(PROP_REPLY_TO, basicContentHeaderProperties.getReplyToAsString());
            builder.property(PROP_EXPIRATION, "" + basicContentHeaderProperties.getExpiration());
            builder.property(PROP_MESSAGE_ID, basicContentHeaderProperties.getMessageIdAsString());
            builder.property(PROP_TIMESTAMP, "" + basicContentHeaderProperties.getTimestamp());
            builder.property(PROP_TYPE, basicContentHeaderProperties.getTypeAsString());
            builder.property(PROP_USER_ID, basicContentHeaderProperties.getUserIdAsString());
            builder.property(PROP_APP_ID, basicContentHeaderProperties.getAppIdAsString());
            builder.property(PROP_CLUSTER_ID, basicContentHeaderProperties.getClusterIdAsString());
            builder.property(PROP_PROPERTY_FLAGS, "" + basicContentHeaderProperties.getPropertyFlags());

            Map<String, Object> headers = basicContentHeaderProperties.getHeadersAsMap();
            for (String key : headers.keySet()) {
                builder.property(BASIC_PROP_HEADER_PRE + key, headers.get(key).toString());
            }
        }

        return (MessageImpl<byte[]>) builder.getMessage();
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

    public static BasicContentHeaderProperties getPropertiesFromMetadata(List<PulsarApi.KeyValue> propertiesList) {
        // TODO convert PulsarApi.KeyValue list to BasicContentHeaderProperties
        return null;
    }

    public static List<Pair<ContentHeaderBody, ContentBody>> entriesToAmqpBodyList(
            List<org.apache.bookkeeper.mledger.Entry> entries) {
        // TODO convert bk entries to amqpbody,
        //  then assemble deliver body with ContentHeaderBody and ContentBody
        return null;
    }

}
