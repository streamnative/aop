


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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class NoRequeueOnCancel extends BrokerTestCase {
    protected final String Q = "NoRequeueOnCancel";

    protected void createResources() throws IOException {
        channel.queueDeclare(Q, false, false, false, null);
    }

    protected void releaseResources() throws IOException {
        channel.queueDelete(Q);
    }

    @Test
    public void noRequeueOnCancel()
            throws IOException, InterruptedException {
        channel.basicPublish("", Q, null, "1".getBytes());

        final CountDownLatch latch = new CountDownLatch(1);
        Consumer c = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                latch.countDown();
            }
        };
        String consumerTag = channel.basicConsume(Q, false, c);
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        channel.basicCancel(consumerTag);

        assertNull(channel.basicGet(Q, true));

        closeChannel();
        openChannel();

        assertNotNull(channel.basicGet(Q, true));
    }
}
