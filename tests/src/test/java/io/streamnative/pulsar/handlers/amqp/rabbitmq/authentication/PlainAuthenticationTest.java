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
package io.streamnative.pulsar.handlers.amqp.rabbitmq.authentication;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.SaslMechanism;
import com.rabbitmq.client.impl.LongStringHelper;
import io.streamnative.pulsar.handlers.amqp.AmqpTokenAuthenticationTestBase;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.testng.annotations.Test;

/**
 * PlainAuthenticationTest tests the plain authentication.
 */
public class PlainAuthenticationTest extends AmqpTokenAuthenticationTestBase {

    private void testConnect(int port) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setUsername("superUser2");
        connectionFactory.setPassword("superpassword");
        @Cleanup
        Connection connection = connectionFactory.newConnection();
        @Cleanup
        Channel ignored = connection.createChannel();
    }

    @Test
    public void testConnectToBroker() throws Exception {
        testConnect(getAmqpBrokerPortList().get(0));
    }

    @Test
    public void testConnectToProxy() throws Exception {
        testConnect(getAopProxyPortList().get(0));
    }

    @Test
    public void testAMQPLAIN() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setPort(getAopProxyPortList().get(0));
        factory.setVirtualHost("vhost1");
        factory.setSaslConfig(mechanisms -> new SaslMechanism() {
            @Override
            public String getName() {
                return "AMQPLAIN";
            }

            @Override
            public LongString handleChallenge(LongString challenge, String username, String password) {
                byte[][] data = new byte[][]{{
                        5, 76, 79, 71, 73, 78, 83, 0, 0, 0, 10, 115, 117, 112, 101, 114, 85, 115,
                        101, 114, 50, 8, 80, 65, 83, 83, 87, 79, 82, 68, 83, 0, 0, 0, 13, 115, 117,
                        112, 101, 114, 112, 97, 115, 115, 119, 111, 114, 100}};
                return LongStringHelper.asLongString(data[0]);
            }
        });
        Connection connection = factory.newConnection();
        connection.close();
    }

    private void testConnectWithInvalidToken(int port, boolean isProxy) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setUsername("superUser2");
        connectionFactory.setPassword("invalidpassword");

        Exception exception;
        if (isProxy) {
            exception = expectThrows(IOException.class,
                    connectionFactory::newConnection);
        } else {
            exception = expectThrows(PossibleAuthenticationFailureException.class,
                    connectionFactory::newConnection);
        }
        assertTrue(exception.getCause().getMessage().contains("Authentication failed"));
    }

    @Test
    public void testConnectToBrokerWithInvalidToken() throws IOException, TimeoutException {
        testConnectWithInvalidToken(getAmqpBrokerPortList().get(0), false);
    }

    @Test
    public void testConnectToProxyWithInvalidToken() throws IOException, TimeoutException {
        testConnectWithInvalidToken(getAopProxyPortList().get(0), true);
    }
}
