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
package io.streamnative.pulsar.handlers.amqp;

import static com.github.fridujo.rabbitmq.mock.configuration.QueueDeclarator.queue;
import static com.github.fridujo.rabbitmq.mock.tool.Exceptions.runAndEatExceptions;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@Slf4j
class IntegrationTest extends AmqpProtocolHandlerTestBase{

    @BeforeEach
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }

        if (!admin.namespaces().getNamespaces("public").contains("public/vhost1")) {
            admin.namespaces().createNamespace("public/vhost1");
            admin.namespaces().setRetention("public/vhost1",
                    new RetentionPolicies(60, 1000));
        }
        Mockito.when(pulsar.getState()).thenReturn(PulsarService.State.Started);
    }

    @AfterEach
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    void basic_consume_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                List<String> messages = new ArrayList<>();
                channel.basicConsume(queueName, false, "myConsumerTag",
                        new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag,
                                                       Envelope envelope,
                                                       AMQP.BasicProperties properties,
                                                       byte[] body) throws IOException {
                                long deliveryTag = envelope.getDeliveryTag();
                                messages.add(new String(body));
                                // (process the message components here ...)
                                channel.basicAck(deliveryTag, false);
                            }
                        });

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

                TimeUnit.MILLISECONDS.sleep(200L);

                assertThat(messages).containsExactly("Hello, world!");
            }
        }
    }

    @Test
    void basic_get_case() throws IOException, TimeoutException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

                GetResponse response = channel.basicGet(queueName, false);
                if (response == null) {
                    fail("AMQP GetReponse must not be null");
                } else {
                    byte[] body = response.getBody();
                    assertThat(new String(body)).isEqualTo("Hello, world!");
                    long deliveryTag = response.getEnvelope().getDeliveryTag();

                    channel.basicAck(deliveryTag, false);
                }
            }
        }
    }

    @Test
    void basic_consume_nack_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = "test-exchange";
        String routingKey = "test.key";

        AtomicInteger atomicInteger = new AtomicInteger();
        final Semaphore waitForAtLeastOneDelivery = new Semaphore(0);
        final Semaphore waitForCancellation = new Semaphore(0);

        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                channel.exchangeDeclare(exchangeName, "direct", true);
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, routingKey);

                channel.basicConsume(queueName, false, "myConsumerTag",
                        new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag,
                                                       Envelope envelope,
                                                       AMQP.BasicProperties properties,
                                                       byte[] body) throws IOException {
                                waitForAtLeastOneDelivery.release();
                                long deliveryTag = envelope.getDeliveryTag();
                                atomicInteger.incrementAndGet();
                                channel.basicNack(deliveryTag, false, true);
                            }

                            @Override
                            public void handleCancel(String consumerTag) {
                                waitForCancellation.release();
                            }
                        });

                byte[] messageBodyBytes = "Hello, world!".getBytes();
                channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
                waitForAtLeastOneDelivery.acquire();
            }
        }

        // WHEN after closing the connection and resetting the counter
        atomicInteger.set(0);

        waitForCancellation.acquire();
        assertThat(atomicInteger.get())
                .describedAs("After connection closed, and Consumer cancellation, "
                        + "no message should be delivered anymore")
                .isZero();
    }

    @Test
    void redelivered_message_should_have_redelivery_marked_as_true() throws IOException, TimeoutException,
            InterruptedException {
        try (Connection conn = getConnection()) {
            CountDownLatch messagesToBeProcessed = new CountDownLatch(2);
            try (Channel channel = conn.createChannel()) {
                queue("fruits").declare(channel);
                AtomicReference<Envelope> redeliveredMessageEnvelope = new AtomicReference();

                channel.basicConsume("fruits", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        if (messagesToBeProcessed.getCount() == 1){
                            redeliveredMessageEnvelope.set(envelope);
                            runAndEatExceptions(messagesToBeProcessed::countDown);

                        } else {
                            runAndEatExceptions(() -> channel.basicNack(envelope.getDeliveryTag(), false, true));
                            runAndEatExceptions(messagesToBeProcessed::countDown);
                        }

                    }
                });

                channel.basicPublish("", "fruits", null, "banana".getBytes());

                final boolean finishedProperly = messagesToBeProcessed.await(1000, TimeUnit.SECONDS);
                assertThat(finishedProperly).isTrue();
                assertThat(redeliveredMessageEnvelope.get()).isNotNull();
                assertThat(redeliveredMessageEnvelope.get().isRedeliver()).isTrue();
            }
        }
    }

    private Connection getConnection() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");
        return connectionFactory.newConnection();
    }
}
