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
import io.streamnative.rabbitmq.authentication.TokenCredentialsProvider;
import io.streamnative.rabbitmq.authentication.TokenSaslConfig;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import lombok.Cleanup;
import org.testng.annotations.Test;

/**
 * TokenAuthenticationTest tests the token authentication.
 */
public class TokenAuthenticationTest extends AmqpTokenAuthenticationTestBase {
    private void testConnect(int port) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setSaslConfig(new TokenSaslConfig());
        connectionFactory.setCredentialsProvider(new TokenCredentialsProvider(() -> clientToken));
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

    private void testConnectWithInvalidToken(int port, boolean isProxy) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setSaslConfig(mechanisms -> {
            String mechanism = "token";
            return new SaslMechanism() {
                @Override
                public String getName() {
                    return mechanism;
                }

                @Override
                public LongString handleChallenge(LongString challenge, String username, String password) {
                    return LongStringHelper.asLongString("");
                }
            };
        });

        Exception exception;
        if (isProxy) {
            exception = expectThrows(IOException.class,
                    connectionFactory::newConnection);
        } else {
            exception = expectThrows(PossibleAuthenticationFailureException.class,
                    connectionFactory::newConnection);
        }

        assertTrue(exception.getCause().getMessage().contains("No authentication data provided"));
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
