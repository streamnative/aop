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

import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 *
 */
public class BasicConsume extends BrokerTestCase {

    @Test
    public void basicConsumeOk() throws IOException, InterruptedException {
        String q = channel.queueDeclare().getQueue();
        basicPublishPersistent("msg".getBytes(StandardCharsets.UTF_8), q);
        basicPublishPersistent("msg".getBytes(StandardCharsets.UTF_8), q);

        CountDownLatch latch = new CountDownLatch(2);
        channel.basicConsume(q, new CountDownLatchConsumer(channel, latch));

        boolean nbOfExpectedMessagesHasBeenConsumed = latch.await(1, TimeUnit.SECONDS);
        assertTrue("Not all the messages have been received", nbOfExpectedMessagesHasBeenConsumed);
    }

    static class CountDownLatchConsumer extends DefaultConsumer {

        private final CountDownLatch latch;

        public CountDownLatchConsumer(Channel channel, CountDownLatch latch) {
            super(channel);
            this.latch = latch;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            latch.countDown();
        }
    }

}
