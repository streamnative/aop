
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
import static org.junit.Assert.fail;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ConsumerCancelNotification extends BrokerTestCase {

    private final String queue = "cancel_notification_queue";

    @Test
    public void consumerCancellationNotification() throws IOException,
            InterruptedException {
        final BlockingQueue<Boolean> result = new ArrayBlockingQueue<Boolean>(1);

        channel.queueDeclare(queue, false, true, false, null);
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleCancel(String consumerTag) throws IOException {
                try {
                    result.put(true);
                } catch (InterruptedException e) {
                    fail();
                }
            }
        };
        channel.basicConsume(queue, consumer);
        channel.queueDelete(queue);
        assertTrue(result.take());
    }

    class AlteringConsumer extends DefaultConsumer {
        private final String altQueue;
        private final CountDownLatch latch;

        public AlteringConsumer(Channel channel, String altQueue, CountDownLatch latch) {
            super(channel);
            this.altQueue = altQueue;
            this.latch = latch;
        }

        @Override
        public void handleShutdownSignal(String consumerTag,
                                         ShutdownSignalException sig) {
            // no-op
        }

        @Override
        public void handleCancel(String consumerTag) {
            try {
                this.getChannel().queueDeclare(this.altQueue, false, true, false, null);
                latch.countDown();
            } catch (IOException e) {
                // e.printStackTrace();
            }
        }
    }

    @Test
    public void consumerCancellationHandlerUsesBlockingOperations()
            throws IOException, InterruptedException {
        final String altQueue = "basic.cancel.fallback";
        channel.queueDeclare(queue, false, true, false, null);

        CountDownLatch latch = new CountDownLatch(1);
        final AlteringConsumer consumer = new AlteringConsumer(channel, altQueue, latch);

        channel.basicConsume(queue, consumer);
        channel.queueDelete(queue);

        latch.await(2, TimeUnit.SECONDS);
    }
}
