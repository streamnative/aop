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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.naming.NamespaceName;
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
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
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

    @Test
    public void testExchangeDeclareFail() {
        String tenant = "public";
        String namespace = "ns";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Init);
        ExchangeDeclareBody cmd = methodRegistry
            .createExchangeDeclareBody(0, "test", "fanout", false, false, false, false, true, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionCloseBody);
    }

    @Test
    public void testExchangeDeclareSuccess() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);

        ExchangeDeclareBody cmd = methodRegistry
            .createExchangeDeclareBody(0, exchange, "fanout", false, true, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
    }

    @Test
    public void testExchangeDeclarePassive(){
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);
        ExchangeDeclareBody cmd1 = methodRegistry
                .createExchangeDeclareBody(0, exchange, "fanout",
                        false, true, false, false, false, null);
        cmd1.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        ExchangeDeclareBody cmd2 = methodRegistry
                .createExchangeDeclareBody(0, exchange, "fanout",
                        true, true, false, false, false, null);
        cmd2.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
    }

    @Test
    public void testQueueDeclarePassive(){
        NamespaceName namespaceName = NamespaceName.get("public", "vhost1");
        connection.setNamespaceName(namespaceName);
        QueueDeclareBody cmd1 = methodRegistry.createQueueDeclareBody(0, "queue", false, true,
                false, false, false, null);
        cmd1.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        QueueDeclareBody cmd2 = methodRegistry.createQueueDeclareBody(0, "queue", true, true,
                false, false, false, null);
        cmd2.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);

    }

    @SneakyThrows
    @Test
    public void testExchangeDelete() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test1";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);

        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, exchange, "fanout", false, false, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        ExchangeDeleteBody cmd1 = methodRegistry.createExchangeDeleteBody(0, exchange, false, true);
        cmd1.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);

        AMQBody response1 = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response1 instanceof ExchangeDeleteOkBody);
    }

    @Test
    public void testExchangeBound() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        List<String> subs = new ArrayList<>();
        subs.add(exchange);
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);

        ExchangeBoundBody cmd = methodRegistry.createExchangeBoundBody(exchange, "key", "queue");
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeBoundOkBody);
    }

    @Test
    public void testQueueDeclare() {
        NamespaceName namespaceName = NamespaceName.get("public", "vhost1");
        connection.setNamespaceName(namespaceName);
        QueueDeclareBody cmd = methodRegistry.createQueueDeclareBody(0, "queue", false, true,
            false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);
    }

    @SneakyThrows
    @Test
    public void testQueueBind() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        String queue = "queue";
        List<String> subs = new ArrayList<>();
        subs.add(exchange);
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);

        exchangeDeclare(exchange, true);

        queueDeclare(queue, true);

        QueueBindBody cmd = methodRegistry.createQueueBindBody(0, queue, exchange, "key",
            false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
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

    @SneakyThrows
    @Test
    public void testQueueUnbind() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        String queue = "queue";
        List<String> subs = new ArrayList<>();
        subs.add(exchange);
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        exchangeDeclare(exchange, true);
        queueDeclare(queue, true);

        QueueUnbindBody cmd = methodRegistry.createQueueUnbindBody(0, AMQShortString.createAMQShortString(queue),
            AMQShortString.createAMQShortString(exchange), AMQShortString.createAMQShortString("key"), null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);

        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueUnbindOkBody);
    }

    @Test
    public void testConfirmSelect() {
        ConfirmSelectBody cmd = new ConfirmSelectBody(false);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConfirmSelectOkBody);
    }



    @Test
    public void testBasicPublish() {

        NamespaceName namespaceName = NamespaceName.get("public", "vhost1");
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);

        final String exchange = "test";
        testConfirmSelect();
        exchangeDeclare(exchange, true);

        BasicPublishBody methodBody = methodRegistry.createBasicPublishBody(0, exchange, "", false, false);
        methodBody.generateFrame(1).writePayload(toServerSender);

        BasicContentHeaderProperties props = new BasicContentHeaderProperties();
        props.setDeliveryMode(Integer.valueOf(BasicContentHeaderProperties.PERSISTENT).byteValue());
        props.setContentType("text/html");
        props.setTimestamp(System.currentTimeMillis());
        props.setHeaders(FieldTableFactory.createFieldTable(Collections.singletonMap("Test", "MST")));

        ContentHeaderBody.createAMQFrame(1, props, contentBytes.length).writePayload(toServerSender);

        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(contentBytes);
        ContentBody contentBody = new ContentBody(qpidByteBuffer);
        ContentBody.createAMQFrame(1, contentBody).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);

//        response = (AMQBody) clientChannel.poll();
//        Assert.assertTrue(response instanceof BasicAckBody);
    }

    private void exchangeDeclare(String exchange, boolean durable) {
        ExchangeDeclareBody exchangeDeclareBody = methodRegistry
            .createExchangeDeclareBody(0, exchange, "fanout", false, durable,
                false, false, false, null);
        exchangeDeclareBody.generateFrame(1).writePayload(toServerSender);
    }

    private void queueDeclare(String queue, boolean durable) {
        QueueDeclareBody queueDeclareBody = methodRegistry.createQueueDeclareBody(0, queue,
            false, durable, false, false, false, null);
        queueDeclareBody.generateFrame(1).writePayload(toServerSender);
    }

    private void queueBind(String exchange, String queue) {
        QueueBindBody queueBindBody = methodRegistry.createQueueBindBody(0, queue, exchange, "key",
            false, null);
        queueBindBody.generateFrame(1).writePayload(toServerSender);
    }

    @Test
    public void testBasicConsume() {
        NamespaceName namespaceName = NamespaceName.get("public", "vhost1");
        connection.setNamespaceName(namespaceName);
        String exchangeName = "ex";
        String queueName = "queue";
        exchangeDeclare(exchangeName, true);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
        queueDeclare(queueName, true);
        toServerSender.flush();
        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueDeclareOkBody);
        queueBind(exchangeName, queueName);
        toServerSender.flush();
        response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof QueueBindOkBody);
        BasicConsumeBody basicConsumeBody = methodRegistry.createBasicConsumeBody(0, "exchangName",
            "consumerTag1", false, true, false, false, null);
        basicConsumeBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        response = (AMQBody) clientChannel.poll();
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
        unacknowledgedMessageMap.add(1, PositionImpl.get(1, 1), consumer, 0);
        unacknowledgedMessageMap.add(2, PositionImpl.get(1, 1), consumer, 0);
        unacknowledgedMessageMap.add(3, PositionImpl.get(1, 1), consumer, 0);
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
        unacknowledgedMessageMap.add(1, PositionImpl.get(1, 1), consumer, 0);
        unacknowledgedMessageMap.add(2, PositionImpl.get(1, 1), consumer, 0);
        unacknowledgedMessageMap.add(3, PositionImpl.get(1, 1), consumer, 0);
        unacknowledgedMessageMap.add(4, PositionImpl.get(1, 1), consumer, 0);
        BasicAckBody basicAckBody = methodRegistry.createBasicAckBody(3, true);
        basicAckBody.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        Assert.assertTrue(unacknowledgedMessageMap.size() == 1);
        Assert.assertTrue(!unacknowledgedMessageMap.getMap().containsKey(3));
    }

}
