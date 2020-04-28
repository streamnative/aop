


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
import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.QueueingConsumer;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

public class Recover extends BrokerTestCase {

    String queue;
    final byte[] body = "message".getBytes();

    public void createResources() throws IOException {
        AMQP.Queue.DeclareOk ok = channel.queueDeclare();
        queue = ok.getQueue();
    }

    interface RecoverCallback {
        void recover(Channel channel) throws IOException;
    }

    // The AMQP specification under-specifies the behaviour when
    // requeue=false.  So we can't really test any scenarios for
    // requeue=false.

    void verifyRedeliverOnRecover(RecoverCallback call)
            throws IOException, InterruptedException {
        QueueingConsumer consumer = new QueueingConsumer(channel);
        channel.basicConsume(queue, false, consumer); // require acks.
        channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().build(), body);
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        assertTrue("consumed message body not as sent",
                Arrays.equals(body, delivery.getBody()));
        // Don't ack it, and get it redelivered to the same consumer
        call.recover(channel);
        QueueingConsumer.Delivery secondDelivery = consumer.nextDelivery(5000);
        assertNotNull("timed out waiting for redelivered message", secondDelivery);
        assertTrue("consumed (redelivered) message body not as sent",
                Arrays.equals(body, delivery.getBody()));
    }

    void verifyNoRedeliveryWithAutoAck(RecoverCallback call)
            throws IOException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<byte[]> bodyReference = new AtomicReference<byte[]>();
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                bodyReference.set(body);
                latch.countDown();
            }
        };
        channel.basicConsume(queue, true, consumer); // auto ack.
        channel.basicPublish("", queue, new AMQP.BasicProperties.Builder().build(), body);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue("consumed message body not as sent",
                Arrays.equals(body, bodyReference.get()));
        call.recover(channel);
        assertNull("should be no message available", channel.basicGet(queue, true));
    }

    final RecoverCallback recoverSync = new RecoverCallback() {
        public void recover(Channel channel) throws IOException {
            channel.basicRecover(true);
        }
    };

    final RecoverCallback recoverSyncConvenience = new RecoverCallback() {
        public void recover(Channel channel) throws IOException {
            channel.basicRecover();
        }
    };

    @Test
    public void redeliveryOnRecover() throws IOException, InterruptedException {
        verifyRedeliverOnRecover(recoverSync);
    }

    @Test
    public void redeliverOnRecoverConvenience()
            throws IOException, InterruptedException {
        verifyRedeliverOnRecover(recoverSyncConvenience);
    }

    @Test
    public void noRedeliveryWithAutoAck()
            throws IOException, InterruptedException {
        verifyNoRedeliveryWithAutoAck(recoverSync);
    }

    @Test
    public void requeueFalseNotSupported() throws Exception {
        try {
            channel.basicRecover(false);
            fail("basicRecover(false) should not be supported");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        }
    }
}
