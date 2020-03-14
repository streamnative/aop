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

import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.AMQBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionSecureBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionStartOkBody;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for AMQP connection level methods.
 */
@Log4j2
public class AmqpConnectionMethodTest extends AmqpProtocolTestBase {

    @Test
    public void testConnectionStartOk() {
        ConnectionStartOkBody cmd = methodRegistry.createConnectionStartOkBody(null,
                AMQShortString.createAMQShortString(""), new byte[0], AMQShortString.createAMQShortString("en_US"));
        cmd.generateFrame(0).writePayload(toServerSender);
        toServerSender.flush();
        AMQBody response = (AMQBody) clientChannel.poll();
        Assert.assertTrue(response instanceof ConnectionSecureBody);
    }
}
