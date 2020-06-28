
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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.QueueingConsumer;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

/**
 * Test Requeue of messages on different types of close.
 * Methods {@link #open} and {@link #close} must be implemented by a concrete subclass.
 */
public abstract class RequeueOnClose
        extends BrokerTestCase {
    private final String q = generateQueueName();
    private final String x = generateExchangeName();
    private final String r = "key-1";
    private final int messageCount = 2;

    protected abstract void open() throws IOException, TimeoutException;

    protected abstract void close() throws IOException;

    public void setUp()
            throws IOException {
        // Override to disable the default behaviour from BrokerTestCase.

    }

    public void tearDown()
            throws IOException {
        // Override to disable the default behaviour from BrokerTestCase.

    }

    private void injectMessage()
            throws IOException {
        channel.basicPublish(x, r, null, "RequeueOnClose message".getBytes());
    }

    private GetResponse getMessage()
            throws IOException {
        return channel.basicGet(q, false);
    }

    private void publishAndGet(int count, boolean doAck)
            throws IOException, InterruptedException, TimeoutException {
        openConnection();
        declareExchangeAndQueueToBind(q, x, r);
        for (int repeat = 0; repeat < count; repeat++) {
            open();
            injectMessage();
            Thread.sleep(200);
            GetResponse r1 = getMessage();
            if (doAck) {
                channel.basicAck(r1.getEnvelope().getDeliveryTag(), false);
            }
            close();
            open();
            if (doAck) {
                assertNull("Expected missing second basicGet (repeat=" + repeat + ")", getMessage());
            } else {
                assertNotNull("Expected present second basicGet (repeat=" + repeat + ")", getMessage());
            }
            close();
        }
        closeConnection();
    }

    /**
     * Test we don't requeue acknowledged messages (using get).
     *
     * @throws Exception untested
     */
    @Test
    public void normal() throws Exception {
        publishAndGet(3, true);
    }

    /**
     * Test we requeue unacknowledged messages (using get).
     *
     * @throws Exception untested
     */
    @Test
    public void requeueing() throws Exception {
        publishAndGet(3, false);
    }

    /**
     * Test we requeue unacknowledged message (using consumer).
     *
     * @throws Exception untested
     */
    @Test
    public void requeueingConsumer() throws Exception {
        openConnection();
        open();
        declareExchangeAndQueueToBind(q, x, r);
        injectMessage();
        QueueingConsumer c = new QueueingConsumer(channel);
        channel.basicConsume(q, c);
        c.nextDelivery();
        close();
        open();
        assertNotNull(getMessage());
        close();
        closeConnection();
    }

    private void publishLotsAndGet()
            throws IOException, InterruptedException, ShutdownSignalException, TimeoutException {
        openConnection();
        open();
        declareExchangeAndQueueToBind(q, x, r);
        for (int i = 0; i < messageCount; i++) {
            channel.basicPublish(x, r, null, "in flight message".getBytes());
        }
        QueueingConsumer c = new QueueingConsumer(channel);
        channel.basicConsume(q, c);
        c.nextDelivery();
        close();
        open();
        for (int i = 0; i < messageCount; i++) {
            assertNotNull("only got " + i + " out of " + messageCount
                    + " messages", channel.basicGet(q, true));
        }
        assertNull("got more messages than " + messageCount + " expected", channel.basicGet(q, true));
        channel.queueDelete(q);
        close();
        closeConnection();
    }

    /**
     * Test close while consuming many messages successfully requeues unacknowledged messages.
     *
     * @throws Exception untested
     */
    @Test
    public void requeueInFlight() throws Exception {
        for (int i = 0; i < 5; i++) {
            publishLotsAndGet();
        }
    }

    /**
     * Test close while consuming partially not acked with cancel successfully requeues unacknowledged messages.
     *
     * @throws Exception untested
     */
    @Test
    public void requeueInFlightConsumerNoAck() throws Exception {
        for (int i = 0; i < 5; i++) {
            publishLotsAndConsumeSome(false, true);
        }
    }

    /**
     * Test close while consuming partially acked with cancel successfully requeues unacknowledged messages.
     *
     * @throws Exception untested
     */
    @Test
    public void requeueInFlightConsumerAck() throws Exception {
        for (int i = 0; i < 5; i++) {
            publishLotsAndConsumeSome(true, true);
        }
    }

    /**
     * Test close while consuming partially not acked without cancel successfully requeues unacknowledged messages.
     *
     * @throws Exception untested
     */
    @Test
    public void requeueInFlightConsumerNoAckNoCancel() throws Exception {
        for (int i = 0; i < 5; i++) {
            publishLotsAndConsumeSome(false, false);
        }
    }

    /**
     * Test close while consuming partially acked without cancel successfully requeues unacknowledged messages.
     *
     * @throws Exception untested
     */
    @Test
    public void requeueInFlightConsumerAckNoCancel() throws Exception {
        for (int i = 0; i < 5; i++) {
            publishLotsAndConsumeSome(true, false);
        }
    }

    private static final int MESSAGES_TO_CONSUME = 2;

    private void publishLotsAndConsumeSome(boolean ack, boolean cancelBeforeFinish)
            throws IOException, InterruptedException, ShutdownSignalException, TimeoutException {
        openConnection();
        open();
        declareExchangeAndQueueToBind(q, x, r);
        for (int i = 0; i < messageCount; i++) {
            channel.basicPublish(x, r, null, "in flight message".getBytes());
        }

        CountDownLatch latch = new CountDownLatch(1);
        PartialConsumer c = new PartialConsumer(channel, MESSAGES_TO_CONSUME, ack, latch, cancelBeforeFinish);
        channel.basicConsume(q, c);
        latch.await();  // wait for consumer

        close();
        open();
        int requeuedMsgCount = (ack) ? messageCount - MESSAGES_TO_CONSUME : messageCount;
        for (int i = 0; i < requeuedMsgCount; i++) {
            assertNotNull("only got " + i + " out of " + requeuedMsgCount + " messages",
                    channel.basicGet(q, true));
        }
        int countMoreMsgs = 0;
        while (null != channel.basicGet(q, true)) {
            countMoreMsgs++;
        }
        assertTrue("got " + countMoreMsgs + " more messages than "
                + requeuedMsgCount + " expected", 0 == countMoreMsgs);
        channel.queueDelete(q);
        close();
        closeConnection();
    }

    private class PartialConsumer extends DefaultConsumer {

        private volatile int count;
        private final Channel channel;
        private final CountDownLatch latch;
        private volatile boolean acknowledge;
        private final boolean cancelBeforeFinish;

        public PartialConsumer(Channel channel, int count, boolean acknowledge,
                               CountDownLatch latch, boolean cancelBeforeFinish) {
            super(channel);
            this.count = count;
            this.channel = channel;
            this.latch = latch;
            this.acknowledge = acknowledge;
            this.cancelBeforeFinish = cancelBeforeFinish;
        }

        @Override
        public void handleDelivery(String consumerTag,
                                   Envelope envelope,
                                   AMQP.BasicProperties properties,
                                   byte[] body)
                throws IOException {
            if (this.acknowledge) {
                this.channel.basicAck(envelope.getDeliveryTag(), false);
            }
            if (--this.count == 0) {
                if (this.cancelBeforeFinish) {
                    this.channel.basicCancel(this.getConsumerTag());
                }
                this.acknowledge = false; // don't acknowledge any more
                this.latch.countDown();
            }
        }
    }
}
