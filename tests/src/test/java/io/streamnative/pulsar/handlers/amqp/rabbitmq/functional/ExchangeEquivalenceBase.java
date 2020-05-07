
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
import java.util.Map;
/**
 * Testcase.
 */
public abstract class ExchangeEquivalenceBase extends BrokerTestCase {
    public void verifyEquivalent(String name,
                                 String type, boolean durable, boolean autoDelete,
                                 Map<String, Object> args) throws IOException {
        channel.exchangeDeclarePassive(name);
        channel.exchangeDeclare(name, type, durable, autoDelete, args);
    }

    // Note: this will close the channel
    public void verifyNotEquivalent(String name,
                                    String type, boolean durable, boolean autoDelete,
                                    Map<String, Object> args) throws IOException {
        channel.exchangeDeclarePassive(name);
        try {
            channel.exchangeDeclare(name, type, durable, autoDelete, args);
            fail("Exchange was supposed to be not equivalent");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ioe);
        }
    }
}
