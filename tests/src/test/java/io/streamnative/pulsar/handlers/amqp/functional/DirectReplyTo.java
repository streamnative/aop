
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class DirectReplyTo extends BrokerTestCase {
    private static final String QUEUE = "amq.rabbitmq.reply-to";

    @Test
    public void roundTrip() throws IOException, InterruptedException {
        QueueMessageConsumer c = new QueueMessageConsumer(channel);
        String replyTo = rpcFirstHalf(c);
        declare(connection, replyTo, true);
        channel.confirmSelect();
        basicPublishVolatile("response".getBytes(), "", replyTo, MessageProperties.BASIC);
        channel.waitForConfirms();

        byte[] body = c.nextDelivery(10000);
        assertEquals("response", new String(body));
    }

    @Test
    public void hack() throws IOException, InterruptedException {
        QueueMessageConsumer c = new QueueMessageConsumer(channel);
        String replyTo = rpcFirstHalf(c);
        // 5 chars should overwrite part of the key but not the pid; aiming to prove
        // we can't publish using just the pid
        replyTo = replyTo.substring(0, replyTo.length() - 5) + "xxxxx";
        declare(connection, replyTo, false);
        basicPublishVolatile("response".getBytes(), "", replyTo, MessageProperties.BASIC);

        byte[] body = c.nextDelivery(500);
        assertNull(body);
    }

    private void declare(Connection connection, String q, boolean expectedExists) throws IOException {
        Channel ch = connection.createChannel();
        try {
            ch.queueDeclarePassive(q);
            assertTrue(expectedExists);
        } catch (IOException e) {
            assertFalse(expectedExists);
            checkShutdownSignal(AMQP.NOT_FOUND, e);
            // Hmmm...
            channel = connection.createChannel();
        }
    }

    @Test
    public void consumeFail() throws IOException, InterruptedException {
        DefaultConsumer c = new DefaultConsumer(channel);
        Channel ch = connection.createChannel();
        try {
            ch.basicConsume(QUEUE, false, c);
        } catch (IOException e) {
            // Can't have ack mode
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }

        ch = connection.createChannel();
        ch.basicConsume(QUEUE, true, c);
        try {
            ch.basicConsume(QUEUE, true, c);
        } catch (IOException e) {
            // Can't have multiple consumers
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    @Test
    public void consumeSuccess() throws IOException, InterruptedException {
        DefaultConsumer c = new DefaultConsumer(channel);
        String ctag = channel.basicConsume(QUEUE, true, c);
        channel.basicCancel(ctag);

        String ctag2 = channel.basicConsume(QUEUE, true, c);
        channel.basicCancel(ctag2);
        assertNotSame(ctag, ctag2);
    }

    private String rpcFirstHalf(Consumer c) throws IOException {
        channel.basicConsume(QUEUE, true, c);
        String serverQueue = channel.queueDeclare().getQueue();
        basicPublishVolatile("request".getBytes(), "", serverQueue, props());

        GetResponse req = channel.basicGet(serverQueue, true);
        return req.getProps().getReplyTo();
    }

    private AMQP.BasicProperties props() {
        return MessageProperties.BASIC.builder().replyTo(QUEUE).build();
    }

    class QueueMessageConsumer extends DefaultConsumer {

        BlockingQueue<byte[]> messages = new LinkedBlockingQueue<byte[]>();

        public QueueMessageConsumer(Channel channel) {
            super(channel);
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            messages.add(body);
        }

        byte[] nextDelivery() {
            return messages.poll();
        }

        byte[] nextDelivery(long timeoutInMs) throws InterruptedException {
            return messages.poll(timeoutInMs, TimeUnit.MILLISECONDS);
        }

    }
}
