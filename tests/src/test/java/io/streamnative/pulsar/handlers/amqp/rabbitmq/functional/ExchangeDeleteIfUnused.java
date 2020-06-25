

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
import java.util.concurrent.TimeoutException;
import org.junit.Test;

/**
 * Testcase.
 * Declare an exchange, bind a queue to it, then try to delete it,
 *  setting if-unused to true.  This should throw an exception.
 */

public class ExchangeDeleteIfUnused extends BrokerTestCase {
    private static final String EXCHANGE_NAME = "xchg1";
    private static final String ROUTING_KEY = "something";

    protected void createResources()
            throws IOException, TimeoutException {
        super.createResources();
        channel.exchangeDeclare(EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY);
    }

    protected void releaseResources()
            throws IOException {
        channel.exchangeDelete(EXCHANGE_NAME);
        super.releaseResources();
    }

    /* Attempt to Exchange.Delete(ifUnused = true) a used exchange.
     * Should throw an exception. */
    @Test
    public void exchangeDelete() {
        try {
            channel.exchangeDelete(EXCHANGE_NAME, true);
            fail("Exception expected if exchange in use");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }
}
