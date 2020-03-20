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
import java.util.List;

import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
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

}
