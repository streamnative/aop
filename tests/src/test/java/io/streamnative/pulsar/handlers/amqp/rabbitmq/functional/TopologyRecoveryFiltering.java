

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

import static com.rabbitmq.client.test.TestUtils.closeAndWaitForRecovery;
import static com.rabbitmq.client.test.TestUtils.exchangeExists;
import static com.rabbitmq.client.test.TestUtils.queueExists;
import static com.rabbitmq.client.test.TestUtils.sendAndConsumeMessage;
import static com.rabbitmq.client.test.TestUtils.waitAtMost;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.impl.recovery.RecordedBinding;
import com.rabbitmq.client.impl.recovery.RecordedConsumer;
import com.rabbitmq.client.impl.recovery.RecordedExchange;
import com.rabbitmq.client.impl.recovery.RecordedQueue;
import com.rabbitmq.client.impl.recovery.TopologyRecoveryFilter;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.TestUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class TopologyRecoveryFiltering extends BrokerTestCase {

    String[] exchangesToDelete = new String[]{
            "recovered.exchange", "filtered.exchange", "topology.recovery.exchange"
    };
    String[] queuesToDelete = new String[]{
            "topology.recovery.queue.1", "topology.recovery.queue.2"
    };
    Connection c;

    @Override
    protected ConnectionFactory newConnectionFactory() {
        ConnectionFactory connectionFactory = TestUtils.connectionFactory();
        connectionFactory.setTopologyRecoveryFilter(new SimpleTopologyRecoveryFilter());
        connectionFactory.setNetworkRecoveryInterval(1000);
        return connectionFactory;
    }

    @Override
    protected void createResources() throws IOException, TimeoutException {
        super.createResources();
        c = connectionFactory.newConnection();
        deleteExchanges(exchangesToDelete);
        deleteQueues(queuesToDelete);
    }

    @Override
    protected void releaseResources() throws IOException {
        super.releaseResources();
        c.close();
        deleteExchanges(exchangesToDelete);
        deleteQueues(queuesToDelete);
    }

    ////@Test
    public void topologyRecoveryFilteringExchangesAndQueues() throws Exception {
        Channel ch = c.createChannel();
        ch.exchangeDeclare("recovered.exchange", "direct");
        ch.exchangeDeclare("filtered.exchange", "direct");
        ch.queueDeclare("recovered.queue", false, true, true, null);
        ch.queueDeclare("filtered.queue", false, true, true, null);

        // to check whether the other connection recovers them or not
        channel.exchangeDelete("recovered.exchange");
        channel.exchangeDelete("filtered.exchange");

        closeAndWaitForRecovery((RecoverableConnection) c);

        assertTrue(exchangeExists("recovered.exchange", c));
        assertFalse(exchangeExists("filtered.exchange", c));

        assertTrue(queueExists("recovered.queue", c));
        assertFalse(queueExists("filtered.queue", c));
    }

    ////@Test
    public void topologyRecoveryFilteringBindings() throws Exception {
        Channel ch = c.createChannel();

        ch.exchangeDeclare("topology.recovery.exchange", "direct");
        ch.queueDeclare("topology.recovery.queue.1", false, false, false, null);
        ch.queueDeclare("topology.recovery.queue.2", false, false, false, null);
        ch.queueBind("topology.recovery.queue.1", "topology.recovery.exchange", "recovered.binding");
        ch.queueBind("topology.recovery.queue.2", "topology.recovery.exchange", "filtered.binding");

        // to check whether the other connection recovers them or not
        channel.queueUnbind("topology.recovery.queue.1", "topology.recovery.exchange", "recovered.binding");
        channel.queueUnbind("topology.recovery.queue.2", "topology.recovery.exchange", "filtered.binding");

        closeAndWaitForRecovery((RecoverableConnection) c);

        assertTrue("The message should have been received by now", sendAndConsumeMessage(
                "topology.recovery.exchange", "recovered.binding", "topology.recovery.queue.1", c
        ));
        assertFalse("Binding shouldn't recover, no messages should have been received", sendAndConsumeMessage(
                "topology.recovery.exchange", "filtered.binding", "topology.recovery.queue.2", c
        ));
    }

    ////@Test
    public void topologyRecoveryFilteringConsumers() throws Exception {
        Channel ch = c.createChannel();

        ch.exchangeDeclare("topology.recovery.exchange", "direct");
        ch.queueDeclare("topology.recovery.queue.1", false, false, false, null);
        ch.queueDeclare("topology.recovery.queue.2", false, false, false, null);
        ch.queueBind("topology.recovery.queue.1", "topology.recovery.exchange", "recovered.consumer");
        ch.queueBind("topology.recovery.queue.2", "topology.recovery.exchange", "filtered.consumer");

        final AtomicInteger recoveredConsumerMessageCount = new AtomicInteger(0);
        ch.basicConsume("topology.recovery.queue.1", true, "recovered.consumer", new DefaultConsumer(ch) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                recoveredConsumerMessageCount.incrementAndGet();
            }
        });
        ch.basicPublish("topology.recovery.exchange", "recovered.consumer", null, "".getBytes());
        waitAtMost(Duration.ofSeconds(5), () -> recoveredConsumerMessageCount.get() == 1);

        final AtomicInteger filteredConsumerMessageCount = new AtomicInteger(0);
        final CountDownLatch filteredConsumerLatch = new CountDownLatch(2);
        ch.basicConsume("topology.recovery.queue.2", true, "filtered.consumer", new DefaultConsumer(ch) {

            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                filteredConsumerMessageCount.incrementAndGet();
                filteredConsumerLatch.countDown();
            }
        });
        ch.basicPublish("topology.recovery.exchange", "filtered.consumer", null, "".getBytes());
        waitAtMost(Duration.ofSeconds(5), () -> filteredConsumerMessageCount.get() == 1);

        closeAndWaitForRecovery((RecoverableConnection) c);

        int initialCount = recoveredConsumerMessageCount.get();
        ch.basicPublish("topology.recovery.exchange", "recovered.consumer", null, "".getBytes());
        waitAtMost(Duration.ofSeconds(5), () -> recoveredConsumerMessageCount.get() == initialCount + 1);

        ch.basicPublish("topology.recovery.exchange", "filtered.consumer", null, "".getBytes());
        assertFalse("Consumer shouldn't recover, no extra messages should have been received",
                filteredConsumerLatch.await(5, TimeUnit.SECONDS));
    }

    private static class SimpleTopologyRecoveryFilter implements TopologyRecoveryFilter {

        @Override
        public boolean filterExchange(RecordedExchange recordedExchange) {
            return !recordedExchange.getName().contains("filtered");
        }

        @Override
        public boolean filterQueue(RecordedQueue recordedQueue) {
            return !recordedQueue.getName().contains("filtered");
        }

        @Override
        public boolean filterBinding(RecordedBinding recordedBinding) {
            return !recordedBinding.getRoutingKey().contains("filtered");
        }

        @Override
        public boolean filterConsumer(RecordedConsumer recordedConsumer) {
            return !recordedConsumer.getConsumerTag().contains("filtered");
        }
    }
}
