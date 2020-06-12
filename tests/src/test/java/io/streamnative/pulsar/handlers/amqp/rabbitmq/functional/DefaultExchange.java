

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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import org.junit.Test;

/**
 * Testcase.
 */
public class DefaultExchange extends BrokerTestCase {
    String queueName = "queue1";

    @Override
    protected void createResources() throws IOException {
        channel.queueDeclare(queueName, true, false, false, null);
    }

    // See bug 22101: publish and declare are the only operations
    // permitted on the default exchange

    @Test
    public void defaultExchangePublish() throws Exception {
        basicPublishVolatile("", queueName); // Implicit binding
        Thread.sleep(2000);
        assertDelivered(queueName, 1);
    }

    @Test
    public void bindToDefaultExchange() {
        try {
            channel.queueBind(queueName, "", "foobar");
            fail();
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, ioe);
        }
    }

    @Test
    public void unbindFromDefaultExchange() {
        try {
            channel.queueUnbind(queueName, "", queueName);
            fail();
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, ioe);
        }
    }

    @Test
    public void declareDefaultExchange() {
        try {
            channel.exchangeDeclare("", "direct", true);
            fail();
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, ioe);
        }
    }

    @Test
    public void deleteDefaultExchange() {
        try {
            channel.exchangeDelete("");
            fail();
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, ioe);
        }
    }
}
