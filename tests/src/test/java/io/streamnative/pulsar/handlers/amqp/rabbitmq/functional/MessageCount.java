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

public class MessageCount extends BrokerTestCase {
    @Test
    public void messageCount() throws IOException {
        String q = generateQueueName();
        channel.queueDeclare(q, false, true, false, null);
        assertEquals(0, channel.messageCount(q));

        basicPublishVolatile(q);
        assertEquals(1, channel.messageCount(q));
        basicPublishVolatile(q);
        assertEquals(2, channel.messageCount(q));

        channel.queuePurge(q);
        assertEquals(0, channel.messageCount(q));
    }
}
