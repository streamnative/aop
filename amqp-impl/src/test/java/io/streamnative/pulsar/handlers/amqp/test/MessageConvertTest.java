package io.streamnative.pulsar.handlers.amqp.test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.bytebuffer.SingleQpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.testng.annotations.Test;


/**
 * test message convert between pulsar entry and amqp body.
 */
public class MessageConvertTest {

    @Test
    private void test() {
        // amqp body to entry
        IncomingMessage incomingMessage = new IncomingMessage(null);
        BasicContentHeaderProperties originProps = new BasicContentHeaderProperties();
        originProps.setTimestamp(System.currentTimeMillis());
        originProps.setContentType("json");

        Map<String, Object> originHeaders = new HashMap<>();
        originHeaders.put("string", "string");
        originHeaders.put("int", 1);
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
        List<Pair<ContentHeaderBody, ContentBody>> pairList = MessageConvertUtils.entriesToAmqpBodyList(Collections.singletonList(entry));
        assertNotNull(pairList);
        assertEquals(1, pairList.size());
        Pair<ContentHeaderBody, ContentBody> pair = pairList.get(0);

        BasicContentHeaderProperties props = pair.getLeft().getProperties();
        Map<String, Object> headers = props.getHeadersAsMap();

        // check
        assertEquals(originProps.getTimestamp(), props.getTimestamp());
        assertEquals(originProps.getContentType(), props.getContentType());
        for (Map.Entry<String, Object> mapEntry : originHeaders.entrySet()) {
            assertEquals(String.valueOf(mapEntry.getValue()), String.valueOf(headers.get(mapEntry.getKey())));
        }

        ByteBuf byteBuf = Unpooled.wrappedBuffer(
                ((SingleQpidByteBuffer) pair.getRight().getPayload()).getUnderlyingBuffer().array());
        assertEquals(expectedContentByteBuf, byteBuf);
    }

}
