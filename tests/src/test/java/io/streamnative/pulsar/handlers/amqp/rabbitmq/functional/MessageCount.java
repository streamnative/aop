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

import static org.junit.Assert.assertEquals;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import org.junit.Test;

/**
 * Testcase.
 */
public class MessageCount extends BrokerTestCase {
    @Test
    public void messageCount() throws IOException, InterruptedException {
        String exchange = generateExchangeName();
        String queue = generateQueueName();
        String routingKey = "key-1";
        declareExchangeAndQueueToBind(queue, exchange, routingKey);
        assertEquals(0, channel.messageCount(queue));

        basicPublishPersistent("1".getBytes(), exchange, routingKey);
        Thread.sleep(200);
        assertEquals(1, channel.messageCount(queue));
        basicPublishPersistent("1".getBytes(), exchange, routingKey);
        Thread.sleep(200);
        assertEquals(2, channel.messageCount(queue));

//        channel.queuePurge(q);
//        assertEquals(0, channel.messageCount(q));
    }
}
