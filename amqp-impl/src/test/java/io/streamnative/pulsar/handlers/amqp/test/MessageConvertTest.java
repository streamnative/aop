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
package io.streamnative.pulsar.handlers.amqp.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageData;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.testng.annotations.Test;


/**
 * test message convert between Pulsar and Amqp.
 */
public class MessageConvertTest {

    @Test
    private void test() throws UnsupportedEncodingException {
        String exchange = "testExchange";
        boolean immediate = true;
        boolean mandatory = false;
        String routingKey = "testRoutingKey";

        // amqp body to entry
        MessagePublishInfo expectedInfo = new MessagePublishInfo();
        expectedInfo.setExchange(AMQShortString.createAMQShortString(exchange));
        expectedInfo.setImmediate(immediate);
        expectedInfo.setMandatory(mandatory);
        expectedInfo.setRoutingKey(AMQShortString.createAMQShortString(routingKey));

        IncomingMessage incomingMessage = new IncomingMessage(expectedInfo);

        BasicContentHeaderProperties originProps = new BasicContentHeaderProperties();
        originProps.setTimestamp(System.currentTimeMillis());
        originProps.setContentType("json");
        originProps.setEncoding("html/json");
        originProps.setPriority((byte) '1');
        Map<String, Object> originHeaders = new HashMap<>();
        originHeaders.put("string", "string");
        originHeaders.put("int", 1);
        originHeaders.put("float", 0.1);
        originHeaders.put("boolean", true);
        originProps.setHeaders(FieldTableFactory.createFieldTable(originHeaders));
        incomingMessage.setContentHeaderBody(new ContentHeaderBody(originProps));

        byte[] singleContent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        int chuckNum = 3;
        ByteBuf expectedContentByteBuf = Unpooled.buffer();
        for (int i = 0; i < chuckNum; i++) {
            incomingMessage.addContentBodyFrame(new ContentBody(QpidByteBuffer.wrap(singleContent)));
            expectedContentByteBuf.writeBytes(singleContent);
        }
        MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(incomingMessage);
        EntryImpl entry = EntryImpl.create(0, 0, MessageConvertUtils.messageToByteBuf(message));

        // entry to amqp body
        List<AmqpMessageData> dataList = MessageConvertUtils.entriesToAmqpBodyList(Collections.singletonList(entry));
        assertNotNull(dataList);
        assertEquals(1, dataList.size());
        AmqpMessageData amqpMessageData = dataList.get(0);

        MessagePublishInfo messagePublishInfo = amqpMessageData.getMessagePublishInfo();
        assertEquals(messagePublishInfo.getExchange().toString(), exchange);
        assertEquals(messagePublishInfo.isImmediate(), immediate);
        assertEquals(messagePublishInfo.isMandatory(), mandatory);
        assertEquals(messagePublishInfo.getRoutingKey().toString(), routingKey);

        BasicContentHeaderProperties props = amqpMessageData.getContentHeaderBody().getProperties();
        Map<String, Object> headers = props.getHeadersAsMap();

        // check
        assertEquals(originProps.getTimestamp(), props.getTimestamp());
        assertEquals(originProps.getContentType(), props.getContentType());
        assertEquals(originProps.getPriority(), props.getPriority());
        assertEquals(originProps.getEncoding(), props.getEncoding());
        for (Map.Entry<String, Object> mapEntry : originHeaders.entrySet()) {
            assertEquals(String.valueOf(mapEntry.getValue()), String.valueOf(headers.get(mapEntry.getKey())));
        }

        ByteBuf byteBuf = Unpooled.wrappedBuffer(
                ((SingleQpidByteBuffer) amqpMessageData.getContentBody().getPayload()).getUnderlyingBuffer().array());
        assertEquals(expectedContentByteBuf, byteBuf);
    }

    @Test
    private void positionConvert() {
        MessageImpl<byte[]> message;
        PositionImpl p1 = PositionImpl.earliest;
        message = MessageConvertUtils.toPulsarMessage(p1);
        PositionImpl p1Converted = MessageConvertUtils.entryToPosition(
                EntryImpl.create(0, 0, MessageConvertUtils.messageToByteBuf(message)));
        assertEquals(p1, p1Converted);

        PositionImpl p2 = PositionImpl.latest;
        message = MessageConvertUtils.toPulsarMessage(p2);
        PositionImpl p2Converted = MessageConvertUtils.entryToPosition(
                EntryImpl.create(0, 0, MessageConvertUtils.messageToByteBuf(message)));
        assertEquals(p2, p2Converted);

        PositionImpl p3 = PositionImpl.get(10, 1);
        message = MessageConvertUtils.toPulsarMessage(p3);
        PositionImpl p3Converted = MessageConvertUtils.entryToPosition(
                EntryImpl.create(0, 0, MessageConvertUtils.messageToByteBuf(message)));
        assertEquals(p3, p3Converted);
    }

}
