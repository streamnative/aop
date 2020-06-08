

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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.QueueingConsumer;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;



/**
 * Nack.
 */
public class Nack extends AbstractRejectTest {

    @Test
    public void singleNack() throws Exception {

        String exchange = generateExchangeName();
        String queue = generateQueueName();
        String routingKey = "key-1";
        declareExchangeAndQueueToBind(queue, exchange, routingKey);
        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();
        basicPublishPersistent(m1, exchange, routingKey);
        basicPublishPersistent(m2, exchange, routingKey);
        Thread.sleep(200);
        long tag1 = checkDelivery(channel.basicGet(queue, false), m1, false);
        long tag2 = checkDelivery(channel.basicGet(queue, false), m2, false);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(queue, false, c);

        // requeue
        channel.basicNack(tag2, false, true);

        long tag3 = checkDelivery(c.nextDelivery(), m2, true);
        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, false, false);

        assertNull(channel.basicGet(queue, false));
        channel.basicAck(tag1, false);
        channel.basicNack(tag3, false, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @Test
    public void multiNack() throws Exception {
        String exchange = generateExchangeName();
        String queue = generateQueueName();
        String routingKey = "key-1";
        declareExchangeAndQueueToBind(queue, exchange, routingKey);
        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();
        byte[] m3 = "3".getBytes();
        byte[] m4 = "4".getBytes();
        basicPublishPersistent(m1, exchange, routingKey);
        basicPublishPersistent(m2, exchange, routingKey);
        basicPublishPersistent(m3, exchange, routingKey);
        basicPublishPersistent(m4, exchange, routingKey);
        Thread.sleep(200);
        checkDelivery(channel.basicGet(queue, false), m1, false);
        long tag1 = checkDelivery(channel.basicGet(queue, false), m2, false);
        checkDelivery(channel.basicGet(queue, false), m3, false);
        long tag2 = checkDelivery(channel.basicGet(queue, false), m4, false);

        // ack, leaving a gap in un-acked sequence
        channel.basicAck(tag1, false);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(queue, false, c);

        // requeue multi
        channel.basicNack(tag2, true, true);

        long tag3 = checkDeliveries(c, m1, m3, m4);

        secondaryChannel.basicCancel(consumerTag);

        // no requeue
        secondaryChannel.basicNack(tag3, true, false);

        assertNull(channel.basicGet(queue, false));

        channel.basicNack(tag3, true, true);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    @Test
    public void nackAll() throws Exception {
        String exchange = generateExchangeName();
        String queue = generateQueueName();
        String routingKey = "key-1";
        declareExchangeAndQueueToBind(queue, exchange, routingKey);
        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();
        basicPublishPersistent(m1, exchange, routingKey);
        basicPublishPersistent(m2, exchange, routingKey);
        Thread.sleep(200);
        long tag1 = checkDelivery(channel.basicGet(queue, false), m1, false);
        long tag2 = checkDelivery(channel.basicGet(queue, false), m2, false);

        // nack all
        channel.basicNack(tag2, true, true);

        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(queue, true, c);

        checkDeliveries(c, m1, m2);

        secondaryChannel.basicCancel(consumerTag);
    }

    private long checkDeliveries(QueueingConsumer c, byte[]... messages)
        throws InterruptedException {

        Set<String> msgSet = new HashSet<String>();
        for (byte[] message : messages) {
            msgSet.add(new String(message));
        }

        long lastTag = -1;
        for (int x = 0; x < messages.length; x++) {
            QueueingConsumer.Delivery delivery = c.nextDelivery();
            String m = new String(delivery.getBody());
            assertTrue("Unexpected message", msgSet.remove(m));
            checkDelivery(delivery, m.getBytes(), true);
            lastTag = delivery.getEnvelope().getDeliveryTag();
        }

        assertTrue(msgSet.isEmpty());
        return lastTag;
    }
}
