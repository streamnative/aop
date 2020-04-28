
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

package io.streamnative.pulsar.handlers.amqp.functional;

import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import org.junit.Test;

/**
 * Test that unbinding from an auto-delete exchange causes the exchange to go
 * away
 */
public class UnbindAutoDeleteExchange extends BrokerTestCase {
    @Test
    public void unbind() throws IOException, InterruptedException {
        String exchange = "myexchange";
        channel.exchangeDeclare(exchange, "fanout", false, true, null);
        String queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, exchange, "");
        channel.queueUnbind(queue, exchange, "");

        try {
            channel.exchangeDeclarePassive(exchange);
            fail("exchange should no longer be there");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
        }
    }
}
