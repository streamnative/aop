

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ConsumerPriorities extends BrokerTestCase {

    @Test
    public void validation() throws IOException {
        assertFailValidation(args("banana"));
        assertFailValidation(args(new HashMap<Object, Object>()));
        assertFailValidation(args(null));
        assertFailValidation(args(Arrays.asList(1, 2, 3)));
    }

    private void assertFailValidation(Map<String, Object> args) throws IOException {
        Channel ch = connection.createChannel();
        String queue = ch.queueDeclare().getQueue();
        try {
            ch.basicConsume(queue, true, args, new DefaultConsumer(ch));
            fail("Validation should fail for " + args);
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ioe);
        }
    }

    private static final int COUNT = 10;
    private static final long DELIVERY_TIMEOUT_MS = 100;
    private static final long CANCEL_OK_TIMEOUT_MS = 10 * 1000;

    @Test
    public void consumerPriorities() throws Exception {
        String queue = channel.queueDeclare().getQueue();
        QueueMessageConsumer highConsumer = new QueueMessageConsumer(channel);
        QueueMessageConsumer medConsumer = new QueueMessageConsumer(channel);
        QueueMessageConsumer lowConsumer = new QueueMessageConsumer(channel);
        String high = channel.basicConsume(queue, true, args(1), highConsumer);
        String med = channel.basicConsume(queue, true, medConsumer);
        channel.basicConsume(queue, true, args(-1), lowConsumer);

        publish(queue, COUNT, "high");
        assertContents(highConsumer, COUNT, "high");
        channel.basicCancel(high);
        assertTrue(
                "High priority consumer should have been cancelled",
                highConsumer.cancelLatch.await(CANCEL_OK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        );
        publish(queue, COUNT, "med");
        assertContents(medConsumer, COUNT, "med");
        channel.basicCancel(med);
        assertTrue(
                "Medium priority consumer should have been cancelled",
                medConsumer.cancelLatch.await(CANCEL_OK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        );
        publish(queue, COUNT, "low");
        assertContents(lowConsumer, COUNT, "low");
    }

    private Map<String, Object> args(Object o) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("x-priority", o);
        return map;
    }

    private void assertContents(QueueMessageConsumer qc, int count, String msg) throws InterruptedException {
        for (int i = 0; i < count; i++) {
            byte[] body = qc.nextDelivery(DELIVERY_TIMEOUT_MS);
            assertEquals(msg, new String(body));
        }
        assertEquals(null, qc.nextDelivery(DELIVERY_TIMEOUT_MS));
    }

    private void publish(String queue, int count, String msg) throws IOException {
        for (int i = 0; i < count; i++) {
            channel.basicPublish("", queue, MessageProperties.MINIMAL_BASIC, msg.getBytes());
        }
    }

    private static class QueueMessageConsumer extends DefaultConsumer {

        BlockingQueue<byte[]> messages = new LinkedBlockingQueue<byte[]>();

        CountDownLatch cancelLatch = new CountDownLatch(1);

        public QueueMessageConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            messages.add(body);
        }

        @Override
        public void handleCancelOk(String consumerTag) {
            cancelLatch.countDown();
        }

        byte[] nextDelivery(long timeoutInMs) throws InterruptedException {
            return messages.poll(timeoutInMs, TimeUnit.MILLISECONDS);
        }

    }
}
