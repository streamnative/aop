
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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static org.junit.Assert.fail;

import com.rabbitmq.client.AuthenticationFailureException;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.LongString;
import com.rabbitmq.client.PossibleAuthenticationFailureException;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.SaslMechanism;
import com.rabbitmq.client.impl.AMQConnection;
import com.rabbitmq.client.impl.LongStringHelper;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.TestUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * SaslMechanisms.
 */
public class SaslMechanisms extends BrokerTestCase {
    private String[] mechanisms;

    /**
     * Mechanism.
     */
    public class Mechanism implements SaslMechanism {
        private final String name;
        private final byte[][] responses;
        private int counter;

        public Mechanism(String name, byte[][] responses) {
            this.name = name;
            this.responses = responses;
        }

        public String getName() {
            return name;
        }

        public LongString handleChallenge(LongString challenge, String username, String password) {
            counter++;
            return LongStringHelper.asLongString(responses[counter - 1]);
        }
    }

    /**
     * Config.
     */
    public class Config implements SaslConfig {
        private final String name;
        private final byte[][] responses;

        public Config(String name, byte[][] responses) {
            this.name = name;
            this.responses = responses;
        }

        public SaslMechanism getSaslMechanism(String[] mechanisms) {
            SaslMechanisms.this.mechanisms = mechanisms;
            return new Mechanism(name, responses);
        }
    }

    ////@Test
    public void plainLogin() throws IOException, TimeoutException {
        loginOk("PLAIN", new byte[][]{"\0guest\0guest".getBytes()});
        loginBad("PLAIN", new byte[][]{"\0guest\0wrong".getBytes()});
    }

    ////@Test
    public void aMQPlainLogin() throws IOException, TimeoutException {
        // guest / guest
        loginOk("AMQPLAIN", new byte[][]{{5, 76, 79, 71, 73, 78, 83, 0, 0, 0, 5, 103,
                117, 101, 115, 116, 8, 80, 65, 83, 83, 87, 79, 82, 68, 83, 0, 0, 0, 5, 103,
                117, 101, 115, 116}});
        // guest / wrong
        loginBad("AMQPLAIN", new byte[][]{{5, 76, 79, 71, 73, 78, 83, 0, 0, 0, 5, 103,
                117, 101, 115, 116, 8, 80, 65, 83, 83, 87, 79, 82, 68, 83, 0, 0, 0, 5, 119,
                114, 111, 110, 103}});
    }

    ////@Test
    public void cRLogin() throws IOException, TimeoutException {
        // Make sure mechanisms is populated
        loginOk("PLAIN", new byte[][]{"\0guest\0guest".getBytes()});

        // We might be running this standalone
        if (Arrays.asList(mechanisms).contains("RABBIT-CR-DEMO")) {
            loginOk("RABBIT-CR-DEMO", new byte[][]{"guest".getBytes(), "My password is guest".getBytes()});
            loginBad("RABBIT-CR-DEMO", new byte[][]{"guest".getBytes(), "My password is wrong".getBytes()});
        }
    }

    ////@Test
    public void connectionCloseAuthFailureUsername() throws IOException, TimeoutException {
        connectionCloseAuthFailure("incorrect-username", "incorrect-password");
    }

    ////@Test
    public void connectionCloseAuthFailurePassword() throws IOException, TimeoutException {
        connectionCloseAuthFailure(connectionFactory.getUsername(), "incorrect-password");
    }

    public void connectionCloseAuthFailure(String username, String password) throws IOException, TimeoutException {
        String failDetail = "for username " + username + " and password " + password;
        try {
            Connection conn = connectionWithoutCapabilities(username, password);
            fail("Expected PossibleAuthenticationFailureException " + failDetail);
            conn.abort();
        } catch (PossibleAuthenticationFailureException paf) {
            if (paf instanceof AuthenticationFailureException) {
                fail("Not expecting AuthenticationFailureException " + failDetail);
            }
        }
    }

    // start a connection without capabilities, causing authentication failures
    // to be reported by the broker by closing the connection
    private Connection connectionWithoutCapabilities(String username, String password)
            throws IOException, TimeoutException {
        ConnectionFactory customFactory = TestUtils.connectionFactory();
        customFactory.setUsername(username);
        customFactory.setPassword(password);
        Map<String, Object> customProperties = AMQConnection.defaultClientProperties();
        customProperties.remove("capabilities");
        customFactory.setClientProperties(customProperties);
        return customFactory.newConnection();
    }

    private void loginOk(String name, byte[][] responses) throws IOException, TimeoutException {
        ConnectionFactory factory = TestUtils.connectionFactory();
        factory.setSaslConfig(new Config(name, responses));
        Connection connection = factory.newConnection();
        connection.close();
    }

    private void loginBad(String name, byte[][] responses) throws IOException, TimeoutException {
        try {
            loginOk(name, responses);
            fail("Login succeeded!");
        } catch (AuthenticationFailureException e) {
            // Ok
        }
    }
}
