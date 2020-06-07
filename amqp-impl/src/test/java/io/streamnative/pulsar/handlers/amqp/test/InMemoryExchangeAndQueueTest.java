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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.impl.FanoutMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryExchange;
import io.streamnative.pulsar.handlers.amqp.impl.InMemoryQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for InMemoryExchange and InMemoryQueue.
 */
public class InMemoryExchangeAndQueueTest {

    @Test
    public void testQueueBind() {
        String exchangeName = "test";
        AmqpExchange exchange = new InMemoryExchange(exchangeName, AmqpExchange.Type.Direct, false);
        AmqpQueue queue = new InMemoryQueue("test", 0);
        queue.bindExchange(exchange, new FanoutMessageRouter(null), "", null);
        Assert.assertNotNull(queue.getRouter(exchangeName));
        Assert.assertEquals(queue.getRouter(exchangeName).getExchange(), exchange);
        Assert.assertEquals(queue.getRouter(exchangeName).getType(), AmqpMessageRouter.Type.Fanout);
        Assert.assertEquals(queue.getRouter(exchangeName).getQueue(), queue);
    }

    @Test
    public void testWriteAndRead() throws ExecutionException, InterruptedException, UnsupportedEncodingException {
        String exchangeName = "test-exchange";
        String queueName = "test-queue";
        AmqpExchange exchange = new InMemoryExchange(exchangeName, AmqpExchange.Type.Direct, false);
        AmqpQueue queue = new InMemoryQueue(queueName, 0);
        queue.bindExchange(exchange, new FanoutMessageRouter(null), "", null);

        byte[] singleContent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        Message<byte[]> message = generateMessage(exchangeName, singleContent);
        ByteBuf expectedContentByteBuf = MessageConvertUtils.messageToByteBuf(message);

        Position position = ((InMemoryExchange) exchange).writeMessageAsync(expectedContentByteBuf).get();
        Assert.assertNotNull(position);

        // Test read entry from exchange directly.
        Entry entry = exchange.readEntryAsync(queueName, position).get();
        Assert.assertNotNull(entry);
        Assert.assertEquals(entry.getDataBuffer(), expectedContentByteBuf);

        // Test read entry from queue.
        PositionImpl p = (PositionImpl) position;
        entry = queue.readEntryAsync(exchangeName, p.getLedgerId(), p.getEntryId()).get();
        Assert.assertNotNull(entry);
        Assert.assertEquals(entry.getDataBuffer(), expectedContentByteBuf);
    }

    @Test
    public void testMarkDelete() throws ExecutionException, InterruptedException, UnsupportedEncodingException {
        String exchangeName = "test-exchange";
        String queueName = "test-queue";
        AmqpExchange exchange = new InMemoryExchange(exchangeName, AmqpExchange.Type.Direct, false);
        AmqpQueue queue = new InMemoryQueue(queueName, 0);
        queue.bindExchange(exchange, new FanoutMessageRouter(null), "", null);
        byte[] singleContent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Position> positions = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            positions.add(exchange.writeMessageAsync(generateMessage(exchangeName, singleContent), "").get());
        }
        Assert.assertEquals(positions.size(), 10);
        for (Position position : positions) {
            Assert.assertNotNull(exchange.readEntryAsync(queueName, position));
        }

        exchange.markDeleteAsync(queueName, positions.get(4)).get();
        for (int i = 0; i < 10; i++) {
            Position position = positions.get(i);
            if (i > 4) {
                Assert.assertNotNull(exchange.readEntryAsync(queueName, position).get());
            } else {
                Assert.assertNull(exchange.readEntryAsync(queueName, position).get());
            }
        }
    }

    @Test
    public void testQueueAcknowledge() throws ExecutionException, InterruptedException, UnsupportedEncodingException {
        String exchangeName = "test-exchange";
        String queueName = "test-queue";
        AmqpExchange exchange = new InMemoryExchange(exchangeName, AmqpExchange.Type.Direct, false);
        AmqpQueue queue = new InMemoryQueue(queueName, 0);
        queue.bindExchange(exchange, new FanoutMessageRouter(null), "", null);
        byte[] singleContent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Position> positions = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            positions.add(exchange.writeMessageAsync(generateMessage(exchangeName, singleContent), "").get());
        }
        Assert.assertEquals(positions.size(), 10);
        for (Position position : positions) {
            Assert.assertNotNull(exchange.readEntryAsync(queueName, position));
        }

        PositionImpl ackPosition = (PositionImpl) positions.get(2);
        queue.acknowledgeAsync(exchangeName, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();
        PositionImpl readPosition = (PositionImpl) positions.get(0);
        Assert.assertNotNull(queue.readEntryAsync(exchangeName, readPosition.getLedgerId(),
                readPosition.getEntryId()).get());
        readPosition = (PositionImpl) positions.get(1);
        Assert.assertNotNull(queue.readEntryAsync(exchangeName, readPosition.getLedgerId(),
                readPosition.getEntryId()).get());
        readPosition = (PositionImpl) positions.get(2);
        Assert.assertNull(queue.readEntryAsync(exchangeName, readPosition.getLedgerId(),
                readPosition.getEntryId()).get());
        readPosition = (PositionImpl) positions.get(3);
        Assert.assertNotNull(queue.readEntryAsync(exchangeName, readPosition.getLedgerId(),
                readPosition.getEntryId()).get());
        ackPosition = (PositionImpl) positions.get(1);
        queue.acknowledgeAsync(exchangeName, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();
        ackPosition = (PositionImpl) positions.get(0);
        queue.acknowledgeAsync(exchangeName, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();

        Position markDelete = exchange.getMarkDeleteAsync(queueName).get();
        Assert.assertEquals(markDelete, positions.get(2));
        Assert.assertEquals(((InMemoryExchange) exchange).getMessages(), 7);
    }

    @Test
    public void testMultipleExchangeQueueBind() {
        String exchangeName1 = "test-exchange-1";
        String exchangeName2 = "test-exchange-2";
        String queueName1 = "test-queue-1";
        String queueName2 = "test-queue-2";
        AmqpExchange exchange1 = new InMemoryExchange(exchangeName1, AmqpExchange.Type.Direct, false);
        AmqpExchange exchange2 = new InMemoryExchange(exchangeName2, AmqpExchange.Type.Direct, false);
        AmqpQueue queue1 = new InMemoryQueue(queueName1, 0);
        AmqpQueue queue2 = new InMemoryQueue(queueName2, 0);
        queue1.bindExchange(exchange1, new FanoutMessageRouter(null), "", null);
        queue1.bindExchange(exchange2, new FanoutMessageRouter(null), "", null);
        queue2.bindExchange(exchange1, new FanoutMessageRouter(null), "", null);
        queue2.bindExchange(exchange2, new FanoutMessageRouter(null), "", null);
        Assert.assertNotNull(queue1.getRouter(exchangeName1));
        Assert.assertNotNull(queue1.getRouter(exchangeName2));
        Assert.assertNotNull(queue2.getRouter(exchangeName1));
        Assert.assertNotNull(queue2.getRouter(exchangeName2));
        Assert.assertEquals(queue1.getRouter(exchangeName1).getExchange(), exchange1);
        Assert.assertEquals(queue1.getRouter(exchangeName2).getExchange(), exchange2);
        Assert.assertEquals(queue2.getRouter(exchangeName1).getExchange(), exchange1);
        Assert.assertEquals(queue2.getRouter(exchangeName2).getExchange(), exchange2);
        Assert.assertEquals(queue1.getRouter(exchangeName1).getType(), AmqpMessageRouter.Type.Fanout);
        Assert.assertEquals(queue1.getRouter(exchangeName2).getType(), AmqpMessageRouter.Type.Fanout);
        Assert.assertEquals(queue2.getRouter(exchangeName1).getType(), AmqpMessageRouter.Type.Fanout);
        Assert.assertEquals(queue2.getRouter(exchangeName2).getType(), AmqpMessageRouter.Type.Fanout);
        Assert.assertEquals(queue1.getRouter(exchangeName1).getQueue(), queue1);
        Assert.assertEquals(queue1.getRouter(exchangeName2).getQueue(), queue1);
        Assert.assertEquals(queue2.getRouter(exchangeName1).getQueue(), queue2);
        Assert.assertEquals(queue2.getRouter(exchangeName2).getQueue(), queue2);
    }

    @Test
    public void testMultipleBindWriteReadAndAcknowledge() throws
            ExecutionException, InterruptedException, UnsupportedEncodingException {
        String exchangeName1 = "test-exchange-1";
        String exchangeName2 = "test-exchange-2";
        String queueName1 = "test-queue-1";
        String queueName2 = "test-queue-2";
        AmqpExchange exchange1 = new InMemoryExchange(exchangeName1, AmqpExchange.Type.Direct, false);
        AmqpExchange exchange2 = new InMemoryExchange(exchangeName2, AmqpExchange.Type.Direct, false);
        AmqpQueue queue1 = new InMemoryQueue(queueName1, 0);
        AmqpQueue queue2 = new InMemoryQueue(queueName2, 0);
        queue1.bindExchange(exchange1, new FanoutMessageRouter(null), "", null);
        queue1.bindExchange(exchange2, new FanoutMessageRouter(null), "", null);
        queue2.bindExchange(exchange1, new FanoutMessageRouter(null), "", null);
        queue2.bindExchange(exchange2, new FanoutMessageRouter(null), "", null);

        byte[] singleContent = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        List<Position> positionsForExchange1 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            positionsForExchange1.add(exchange1.writeMessageAsync(
                    generateMessage(exchangeName1, singleContent), "").get());
        }

        List<Position> positionsForExchange2 = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            positionsForExchange2.add(exchange2.writeMessageAsync(
                    generateMessage(exchangeName2, singleContent), "").get());
        }

        PositionImpl ackPosition = (PositionImpl) positionsForExchange1.get(2);
        queue1.acknowledgeAsync(exchangeName1, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();
        Assert.assertNull(queue1.readEntryAsync(exchangeName1, ackPosition.getLedgerId(),
                ackPosition.getEntryId()).get());
        Assert.assertNotNull(queue1.readEntryAsync(exchangeName2, ackPosition.getLedgerId(),
                ackPosition.getEntryId()).get());
        Assert.assertNotNull(queue2.readEntryAsync(exchangeName1, ackPosition.getLedgerId(),
                ackPosition.getEntryId()).get());
        Assert.assertNotNull(queue2.readEntryAsync(exchangeName2, ackPosition.getLedgerId(),
                ackPosition.getEntryId()).get());

        ackPosition = (PositionImpl) positionsForExchange1.get(0);
        queue1.acknowledgeAsync(exchangeName1, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();
        Assert.assertEquals(exchange1.getMarkDeleteAsync(queueName1).get(), positionsForExchange1.get(0));
        Assert.assertEquals(((InMemoryExchange) exchange1).getMessages(), 10);

        queue2.acknowledgeAsync(exchangeName1, ackPosition.getLedgerId(), ackPosition.getEntryId()).get();
        Assert.assertEquals(((InMemoryExchange) exchange1).getMessages(), 9);
    }

    private Message<byte[]> generateMessage(String exchangeName, byte[] payload) throws UnsupportedEncodingException {

        boolean immediate = true;
        boolean mandatory = false;
        String routingKey = "testRoutingKey";

        MessagePublishInfo publishInfo = new MessagePublishInfo();
        publishInfo.setExchange(AMQShortString.createAMQShortString(exchangeName));
        publishInfo.setImmediate(immediate);
        publishInfo.setMandatory(mandatory);
        publishInfo.setRoutingKey(AMQShortString.createAMQShortString(routingKey));

        IncomingMessage incomingMessage = new IncomingMessage(publishInfo);

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
        incomingMessage.addContentBodyFrame(new ContentBody(QpidByteBuffer.wrap(payload)));

        return MessageConvertUtils.toPulsarMessage(incomingMessage);
    }
}
