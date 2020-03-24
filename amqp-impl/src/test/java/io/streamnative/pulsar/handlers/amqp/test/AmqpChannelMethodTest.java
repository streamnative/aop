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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTableFactory;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
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

    @Test
    public void testExchangeDeclareFail() {
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Init);
        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, "test", "fanout", true, true, false, false, false, null);
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
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));
        Mockito.when(connection.getExchangeTopicManager().
                getOrCreateTopic(String.format("persistent://%s/%s/%s", tenant, namespace, exchange), true)).
                thenReturn(Mockito.mock(Topic.class));
        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, exchange, "fanout", true, true, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
    }

    @SneakyThrows
    @Test
    public void testExchangeDelete() {
        String tenant = "public";
        String namespace = "ns";
        String exchange = "test";
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        connection.setNamespaceName(namespaceName);
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));

        Topic topic = Mockito.mock(Topic.class);
        Mockito.when(connection.getExchangeTopicManager().
                getOrCreateTopic(String.format("persistent://%s/%s/%s", tenant, namespace, exchange), false)).
                thenReturn(topic);

        Mockito.when(topic.delete()).thenReturn(Mockito.mock(CompletableFuture.class));
        Mockito.when(topic.delete().get()).thenReturn(null);

        ExchangeDeleteBody cmd = methodRegistry.createExchangeDeleteBody(0, exchange, false, true);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeleteOkBody);
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
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));

        Topic topic = Mockito.mock(Topic.class);
        Mockito.when(connection.getExchangeTopicManager().
                getOrCreateTopic(String.format("persistent://%s/%s/%s", tenant, namespace, exchange), false)).
                thenReturn(topic);
        Mockito.when(topic.getSubscriptions()).thenReturn(Mockito.mock(ConcurrentOpenHashMap.class));
        Mockito.when(topic.getSubscriptions().keys()).thenReturn(subs);

        ExchangeBoundBody cmd = methodRegistry.createExchangeBoundBody(exchange, "key", "queue");
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
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));

        Topic topic = Mockito.mock(Topic.class);
        Mockito.when(connection.getExchangeTopicManager().
                getOrCreateTopic(String.format("persistent://%s/%s/%s", tenant, namespace, exchange), false)).
                thenReturn(topic);
        Mockito.when(topic.createSubscription(queue, PulsarApi.CommandSubscribe.InitialPosition.Earliest,
                false)).thenReturn(Mockito.mock(CompletableFuture.class));
        Mockito.when(topic.createSubscription(queue, PulsarApi.CommandSubscribe.InitialPosition.Earliest,
                false).get()).thenReturn(null);

        QueueBindBody cmd = methodRegistry.createQueueBindBody(0, queue, exchange, "key",
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
        Mockito.when(connection.getAmqpConfig().getAmqpTenant()).thenReturn(tenant);
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));

        Topic topic = Mockito.mock(Topic.class);
        Mockito.when(connection.getExchangeTopicManager().
                getOrCreateTopic(String.format("persistent://%s/%s/%s", tenant, namespace, exchange), false)).
                thenReturn(topic);
        Mockito.when(topic.getSubscription(queue)).thenReturn(Mockito.mock(Subscription.class));
        Mockito.when(topic.getSubscription(queue).delete()).thenReturn(Mockito.mock(CompletableFuture.class));
        Mockito.when(topic.getSubscription(queue).delete().get()).thenReturn(null);

        QueueUnbindBody cmd = methodRegistry.createQueueUnbindBody(0, AMQShortString.createAMQShortString(queue),
                AMQShortString.createAMQShortString(exchange), AMQShortString.createAMQShortString("key"), null);
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

        ContentHeaderBody headerBody = new ContentHeaderBody(props, contentBytes.length);
        ContentHeaderBody.createAMQFrame(1, props, contentBytes.length).writePayload(toServerSender);

        QpidByteBuffer qpidByteBuffer = QpidByteBuffer.wrap(contentBytes);
        ContentBody contentBody = new ContentBody(qpidByteBuffer);
        ContentBody.createAMQFrame(1, contentBody).writePayload(toServerSender);
        toServerSender.flush();

        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof BasicAckBody);
    }

}
