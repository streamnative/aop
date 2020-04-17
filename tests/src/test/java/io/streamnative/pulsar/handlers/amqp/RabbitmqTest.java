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

import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * RabbitMQ client test.
 */
@Slf4j
public class RabbitmqTest extends AmqpProtocolHandlerTestBase {

    @BeforeMethod
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

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 5000)
    private void inMemoryE2ETest() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");

        final String exchangeName = "ex1";
        final String queueName1 = "ex1-q1";
        final String queueName2 = "ex1-q2";
        final boolean durable = false;

        @Cleanup
        Connection connection = connectionFactory.newConnection();

        @Cleanup
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, durable);
        channel.queueDeclare(queueName1, durable, false, false, null);
        channel.queueDeclare(queueName2, durable, false, false, null);

        channel.queueBind(queueName1, exchangeName, "");
        channel.queueBind(queueName2, exchangeName, "");

        final String str = "Hello AMQP";
        channel.basicPublish(exchangeName, "", null, (str).getBytes(StandardCharsets.UTF_8));

        CountDownLatch countDownLatch = new CountDownLatch(2);
        channel.basicConsume(queueName1, (consumeTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info("{} receive: {}", queueName1, message);
            Assert.assertEquals(message, str);
            countDownLatch.countDown();
        }, consumerTag -> {
        });

        channel.basicConsume(queueName2, (consumeTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            log.info("{} receive: {}", queueName2, message);
            Assert.assertEquals(message, str);
            countDownLatch.countDown();
        }, consumerTag -> {
        });
        countDownLatch.await();
    }

    @Test
    private void basicPublishTest() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");

        final String queueName = "testQueue";
        final String message = "Hello AOP!";
        final int messagesNum = 10;

        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://127.0.0.1:"
            + brokerWebservicePort).build();
        log.info("topics: {}", pulsarAdmin.topics());

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            for (int i = 0; i < messagesNum; i++) {
                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println("send message: " + message);
            }
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:" + brokerPort).build();
        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic("persistent://public/vhost1/" + AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE)
            .subscriptionName("test")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        for (int i = 0; i < messagesNum; i++) {
            Message<byte[]> msg = consumer.receive();
            System.out.println("receive msg: " + new String(msg.getData()));
            Assert.assertEquals(new String(msg.getData()), message);
        }
    }

    @Test
    private void persistentExchangeAndQueueWriteTest() throws IOException, TimeoutException {
        final String vhost = "vhost1";
        final String exchangeName = "ex";
        final String queueName1 = "ex-q1";
        final String queueName2 = "ex-q2";

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost(vhost);

        @Cleanup
        Connection connection = connectionFactory.newConnection();
        @Cleanup
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(queueName1, true, false, false, null);
        channel.queueBind(queueName1, exchangeName, "");
        channel.queueDeclare(queueName2, true, false, false, null);
        channel.queueBind(queueName2, exchangeName, "");

        String contentMsg = "Hello AOP!";
        channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:" + brokerPort).build();
        String exchangeTopic = "persistent://public/vhost1/ex";
        NamespaceName namespaceName = NamespaceName.get("public", vhost);
        String queueIndexTopic1 = PersistentQueue.getIndexTopicName(namespaceName, queueName1);
        String queueIndexTopic2 = PersistentQueue.getIndexTopicName(namespaceName, queueName2);

        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> exchangeConsumer =
            pulsarClient.newConsumer()
                .topic(exchangeTopic)
                .subscriptionName("test-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> queueIndexConsumer1 =
            pulsarClient.newConsumer()
                .topic(queueIndexTopic1)
                .subscriptionName("test-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> queueIndexConsumer2 =
            pulsarClient.newConsumer()
                .topic(queueIndexTopic2)
                .subscriptionName("test-sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<byte[]> message = exchangeConsumer.receive();
        final long ledgerId = ((MessageIdImpl) message.getMessageId()).getLedgerId();
        final long entryId = ((MessageIdImpl) message.getMessageId()).getEntryId();
        log.info("[{}] receive messageId: {}, msg: {}",
            exchangeTopic, message.getMessageId(), new String(message.getData()));
        Assert.assertEquals(new String(message.getData()), contentMsg);

        message = queueIndexConsumer1.receive();
        ByteBuf byteBuf1 = Unpooled.wrappedBuffer(message.getData());
        Assert.assertEquals(ledgerId, byteBuf1.readLong());
        Assert.assertEquals(entryId, byteBuf1.readLong());

        message = queueIndexConsumer2.receive();
        ByteBuf byteBuf2 = Unpooled.wrappedBuffer(message.getData());
        Assert.assertEquals(ledgerId, byteBuf2.readLong());
        Assert.assertEquals(entryId, byteBuf2.readLong());
    }

    @Test
    private void fanoutConsumeTest() throws IOException, TimeoutException, InterruptedException {

        final String vhost = "vhost1";
        final String exchangeName = "ex1";
        final String queueName1 = "ex1-q1";
        final String queueName2 = "ex1-q2";
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost(vhost);
        @Cleanup
        Connection connection = connectionFactory.newConnection();
        @Cleanup
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(queueName1, true, false, false, null);
        channel.queueBind(queueName1, exchangeName, "");
        channel.queueDeclare(queueName2, true, false, false, null);
        channel.queueBind(queueName2, exchangeName, "");

        String contentMsg = "Hello AOP!";
        channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());

        final AtomicInteger count = new AtomicInteger(0);

        @Cleanup
        Channel channel1 = connection.createChannel();
        Consumer consumer1 = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received Consumer '" + message + "'");
                count.incrementAndGet();
            }
        };
        channel1.basicConsume(queueName1, true, consumer1);

        @Cleanup
        Channel channel2 = connection.createChannel();
        Consumer consumer2 = new DefaultConsumer(channel2) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" [x] Received Consumer '" + message + "'");
                count.incrementAndGet();
            }
        };
        channel2.basicConsume(queueName2, true, consumer2);
        Thread.sleep(1000);
        Assert.assertTrue(count.get() == 2);

    }

}
