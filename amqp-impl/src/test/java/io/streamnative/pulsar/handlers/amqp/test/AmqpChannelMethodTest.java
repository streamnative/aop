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

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestBody;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicGetOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
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
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));
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
        Mockito.when(connection.getPulsarService().getBrokerService()).thenReturn(Mockito.mock(BrokerService.class));
        Mockito.when(connection.getPulsarService().getState()).thenReturn(PulsarService.State.Started);
        Mockito.when(connection.getPulsarService().getBrokerService().getOrCreateTopic("test")).thenReturn(null);
        ExchangeDeclareBody cmd = methodRegistry
                .createExchangeDeclareBody(0, "test", "fanout", true, true, false, false, false, null);
        cmd.generateFrame(1).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ExchangeDeclareOkBody);
    }
}
