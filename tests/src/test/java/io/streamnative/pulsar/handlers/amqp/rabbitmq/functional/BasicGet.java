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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.junit.Test;



/**
 * Testcase.
 */
public class BasicGet extends BrokerTestCase {

    @Test
    public void basicGetWithEmptyQueue() throws IOException, InterruptedException {
        String queueName = "qu-2";
        assertTrue(channel.isOpen());
        channel.queueDeclare(queueName, true, false, false, null);
        Thread.sleep(250);
        assertNull(channel.basicGet(queueName, true));
        channel.queueDelete(queueName);
    }

    @Test
    public void basicGetWithClosedChannel() throws IOException, InterruptedException, TimeoutException {
        assertTrue(channel.isOpen());
        String q = channel.queueDeclare().getQueue();

        channel.close();
        assertFalse(channel.isOpen());
        try {
            channel.basicGet(q, true);
            fail("expected basic.get on a closed channel to fail");
        } catch (AlreadyClosedException e) {
            // passed
        } finally {
            Channel tch = connection.createChannel();
            tch.queueDelete(q);
            tch.close();
        }

    }

    @Test
    public void basicGetWithEnqueuedMessagesbasicGetWithEmptyQueue() throws IOException, InterruptedException {
        String exchangeName = "ex-1";
        String queueName = "qu-1";
        String routingKey = "key-2";
        assertTrue(channel.isOpen());
        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);
        for (int i = 0; i < 10; i++) {
            basicPublishPersistent(exchangeName, routingKey);
            Thread.sleep(250);
            assertNotNull(channel.basicGet(queueName, true));
        }
        channel.queueDelete(queueName);

    }


}
