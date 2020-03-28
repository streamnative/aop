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

import io.streamnative.pulsar.handlers.amqp.AmqpChannel;
import io.streamnative.pulsar.handlers.amqp.AmqpConsumer;
import io.streamnative.pulsar.handlers.amqp.UnacknowledgedMessageMap;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicCancelOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicPublishBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueuePurgeBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for AMQP channel level methods.
 */
public class AmqpChannelMethodTest extends AmqpProtocolTestBase {

    @Test
    public void testBasicGet() {
        BasicGetBody cmd = methodRegistry.createBasicGetBody(1, AMQShortString.createAMQShortString("test"), false);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof BasicGetOkBody);
        BasicGetOkBody basicGetOkBody = (BasicGetOkBody) response;
        Assert.assertEquals(basicGetOkBody.getMessageCount(), 100);
    }

    @Test
    public void testAccessRequest() {
        AccessRequestBody cmd = methodRegistry
                .createAccessRequestBody(AMQShortString.createAMQShortString("test"), true, false, true, true, true);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof AccessRequestOkBody);
        AccessRequestOkBody accessRequestOkBody = (AccessRequestOkBody) response;
        Assert.assertEquals(accessRequestOkBody.getTicket(), 0);
    }

    @SneakyThrows
    @Test
    public void testExchangeDeclareFail() {
        Mockito.when(connection.getPulsarService().getAdminClient()).thenReturn(Mockito.mock(PulsarAdmin.class));
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Init);
        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, "test", "fanout", true, true, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionCloseBody);
    }

    @SneakyThrows
    @Test
    public void testExchangeDeclareSuccess() {
        Mockito.when(connection.getPulsarService().getAdminClient()).thenReturn(Mockito.mock(PulsarAdmin.class));
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);
        Mockito.when(connection.getPulsarService().getAdminClient().topics()).thenReturn(Mockito.mock(Topics.class));
        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, "test", "fanout", true, true, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
    }

    @SneakyThrows
    @Test
    public void testExchangeDelete() {
        List<String> topics = new ArrayList<>();
        topics.add("test1");
        topics.add("test2");
        List<String> queues = new ArrayList<>();
        queues.add("queue1");
        queues.add("queue2");
        Mockito.when(connection.getPulsarService().getAdminClient()).thenReturn(Mockito.mock(PulsarAdmin.class));
        Mockito.when(connection.getPulsarService().getAdminClient().topics()).thenReturn(Mockito.mock(Topics.class));
        Mockito.when(connection.getPulsarService().getAdminClient().topics().getList("")).thenReturn(topics);
        Mockito.when(connection.getPulsarService().getAdminClient().topics().getSubscriptions("test1"))
                .thenReturn(queues);
        ExchangeDeleteBody cmd = methodRegistry.createExchangeDeleteBody(0, "test1", false, true);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeleteOkBody);
    }

    @Test
    public void testExchangeBound() {
        ExchangeBoundBody cmd = methodRegistry.createExchangeBoundBody("test", "key", "queue");
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeBoundOkBody);
    }

    @Test
    public void testQueueDeclare() {
        QueueDeclareBody cmd = methodRegistry.createQueueDeclareBody(0, "queue", false, true,
                false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);
    }

    @Test
    public void testQueueBind() {
        QueueBindBody cmd = methodRegistry.createQueueBindBody(0, "queue", "test", "key",
                false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueBindOkBody);
    }

    @Test
    public void testQueuePurge() {
        QueuePurgeBody cmd = methodRegistry
                .createQueuePurgeBody(0, AMQShortString.createAMQShortString("queue"), false);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionCloseBody);
    }

    @Test
    public void testQueueDelete() {
        QueueDeleteBody cmd = methodRegistry.createQueueDeleteBody(0, "queue", false, false,
                false);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeleteOkBody);
    }

    @Test
    public void testQueueUnbind() {
        QueueUnbindBody cmd = methodRegistry.createQueueUnbindBody(0, AMQShortString.createAMQShortString("queue"),
                AMQShortString.createAMQShortString("test"), AMQShortString.createAMQShortString("key"), null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueUnbindOkBody);
    }

    @Test
    public void testBasicPublish() {
        BasicPublishBody methodBody = methodRegistry.createBasicPublishBody(0, "test", "", false, false);
        methodBody.generateFrame(1).writePayload(toServerSender);

        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setDeliveryMode(Integer.valueOf(BasicContentHeaderProperties.PERSISTENT).byteValue());
        props.setContentType("text/html");
        props.setTimestamp(System.currentTimeMillis());
        props.setHeaders(FieldTableFactory.createFieldTable(Collections.singletonMap("Test", "MST")));
        //props.set

        ContentHeaderBody headerBody = new ContentHeaderBody(props, contentBytes.length);
        ContentHeaderBody.createAMQFrame(1, props, contentBytes.length).writePayload(toServerSender);

        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(contentBytes);
        ContentBody contentBody = new ContentBody(qpidByteBuffer);
        ContentBody.createAMQFrame(1, contentBody).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof BasicAckBody);
    }

    @Test
    public void testBasicConsume() {
        SocketAddress socketAddress = Mockito.mock(SocketAddress.class);
        Mockito.when(connection.getServerCnx().clientAddress()).thenReturn(socketAddress);
        BasicConsumeBody basicConsumeBody = methodRegistry.createBasicConsumeBody(0, "exchangName",
            "consumerTag1", false, true, false, false, null);
        basicConsumeBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof BasicConsumeOkBody);
    }

    @Test
    public void testBasicCancel() {
        testBasicConsume();
        BasicCancelBody basicCancelBody = methodRegistry.
            createBasicCancelBody(AMQShortString.createAMQShortString("consumerTag1"), false);
        basicCancelBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof BasicCancelOkBody);
    }

    @Test
    public void testBasicAckOne() {
        testBasicConsume();
        AmqpChannel channel = (AmqpChannel) connection.getChannelMethodProcessor(0);
        AmqpConsumer consumer = (AmqpConsumer) channel.getTag2ConsumersMap().get("consumerTag1");
        Assert.assertTrue(consumer != null);
        UnacknowledgedMessageMap unacknowledgedMessageMap = channel.getUnacknowledgedMessageMap();
        unacknowledgedMessageMap.add(1, PositionImpl.get(1, 1), consumer);
        unacknowledgedMessageMap.add(2, PositionImpl.get(1, 1), consumer);
        unacknowledgedMessageMap.add(3, PositionImpl.get(1, 1), consumer);
        BasicAckBody basicAckBody = methodRegistry.createBasicAckBody(1, false);
        basicAckBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        Assert.assertTrue(unacknowledgedMessageMap.size() == 2);
        Assert.assertTrue(!unacknowledgedMessageMap.getMap().containsKey(1));
    }

    @Test
    public void testBasicAckBatch() {
        testBasicConsume();
        AmqpChannel channel = (AmqpChannel) connection.getChannelMethodProcessor(0);
        AmqpConsumer consumer = (AmqpConsumer) channel.getTag2ConsumersMap().get("consumerTag1");
        Assert.assertTrue(consumer != null);
        UnacknowledgedMessageMap unacknowledgedMessageMap = channel.getUnacknowledgedMessageMap();
        unacknowledgedMessageMap.add(1, PositionImpl.get(1, 1), consumer);
        unacknowledgedMessageMap.add(2, PositionImpl.get(1, 1), consumer);
        unacknowledgedMessageMap.add(3, PositionImpl.get(1, 1), consumer);
        unacknowledgedMessageMap.add(4, PositionImpl.get(1, 1), consumer);
        BasicAckBody basicAckBody = methodRegistry.createBasicAckBody(3, true);
        basicAckBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        Assert.assertTrue(unacknowledgedMessageMap.size() == 1);
        Assert.assertTrue(!unacknowledgedMessageMap.getMap().containsKey(3));
    }

}
