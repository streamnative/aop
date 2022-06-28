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
package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpTestBase;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * RabbitMQ messaging test.
 */
@Slf4j
public class RabbitMQMessagingTest extends AmqpTestBase {

    @DataProvider(name = "consumeExclusiveProvider")
    public Object[][] consumeExclusiveProvider() {
        return new Object[][]{
                { false }, { true }
        };
    }

    @Test(timeOut = 1000 * 5)
    public void basicConsumeCaseWithNewAddedHost() throws Exception {
        String newAddedVhost = "vhost_new_added";
        Connection unknownConn = getConnection(newAddedVhost, false);
        Channel unknownChannel = unknownConn.createChannel();

        try {
            unknownChannel.exchangeDeclare(randExName(), "direct", true);
            Assert.fail("Should failed to declare exchange with unknown exchange.");
        } catch (Exception e) {
            log.info("ignored errors");
        }

        admin.namespaces().createNamespace("public/" + newAddedVhost);
        if (unknownConn.isOpen()) {
            unknownConn.close();
        }
        basicDirectConsume(newAddedVhost, false);
    }

    @Test(timeOut = 1000 * 5, dataProvider = "consumeExclusiveProvider")
    public void basicConsumeCase(boolean consumeExclusive) throws Exception {
        basicDirectConsume("vhost1", consumeExclusive);
    }

    @Test(timeOut = 1000 * 5)
    void basicConsumeNackCase() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();
        AtomicInteger atomicInteger = new AtomicInteger();
        final Semaphore waitForAtLeastOneDelivery = new Semaphore(0);
        @Cleanup
        Connection conn = getConnection("vhost1", false);
        @Cleanup
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        channel.basicConsume(queueName, false,
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
//                                waitForCancellation.release();
                }
            });

        byte[] messageBodyBytes = "Hello, world!".getBytes();
        channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        waitForAtLeastOneDelivery.acquire();

        // WHEN after closing the connection and resetting the counter
        atomicInteger.set(0);

//        waitForCancellation.acquire();

        // After connection closed, and Consumer cancellation, no message should be delivered anymore
        Assert.assertEquals(atomicInteger.get(), 0);
    }

    @Test(timeOut = 1000 * 5)
    void redeliveredMessageShouldHaveRedeliveryMarkedAsTrue() throws IOException, TimeoutException,
        InterruptedException {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();
        @Cleanup
        Connection conn = getConnection("vhost1", false);
        CountDownLatch messagesToBeProcessed = new CountDownLatch(2);
        @Cleanup
        Channel channel = conn.createChannel();
        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);
        AtomicReference<Envelope> redeliveredMessageEnvelope = new AtomicReference();

        channel.basicConsume(queueName, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                Envelope envelope,
                AMQP.BasicProperties properties,
                byte[] body) throws IOException {
                if (messagesToBeProcessed.getCount() == 1) {
                    redeliveredMessageEnvelope.set(envelope);
                    messagesToBeProcessed.countDown();

                } else {
                    channel.basicNack(envelope.getDeliveryTag(), false, true);
                    messagesToBeProcessed.countDown();
                }

            }
        });

        channel.basicPublish(exchangeName, routingKey, null, "banana".getBytes());

        final boolean finishedProperly = messagesToBeProcessed.await(1000, TimeUnit.SECONDS);
        Assert.assertTrue(finishedProperly);
        Assert.assertNotNull(redeliveredMessageEnvelope.get());
        Assert.assertTrue(redeliveredMessageEnvelope.get().isRedeliver());

    }

    @Test(timeOut = 1000 * 5)
    private void basicPublishTest() throws IOException, TimeoutException {
        final String queueName = randQuName();
        final String message = "Hello AOP!";
        final int messagesNum = 10;

        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://127.0.0.1:"
            + getBrokerWebservicePortList().get(0)).build();
        log.info("topics: {}", pulsarAdmin.topics());

        try (Connection connection = getConnection("vhost1", false);
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queueName, true, false, false, null);
            for (int i = 0; i < messagesNum; i++) {
                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println("send message: " + message);
            }
        }

        String exchangeTopicName = PersistentExchange.getExchangeTopicName(
                NamespaceName.get("public", "vhost1"),
                AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE);
        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
            .serviceUrl("pulsar://localhost:" + getBrokerPortList().get(0)).build();
        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(exchangeTopicName)
            .subscriptionName("test")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        for (int i = 0; i < messagesNum; i++) {
            Message<byte[]> msg = consumer.receive();
            System.out.println("receive msg: " + new String(msg.getData()));
            Assert.assertEquals(new String(msg.getData()), message);
        }
    }

    @Test(timeOut = 1000 * 5)
    private void persistentExchangeAndQueueWriteTest() throws IOException, TimeoutException {
        final String vhost = "vhost1";
        final String exchangeName = randExName();
        final String queueName1 = randQuName();
        final String queueName2 = randQuName();

        @Cleanup
        Connection connection = getConnection("vhost1", false);
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
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(
                "pulsar://localhost:" + getBrokerPortList().get(0)).build();
        NamespaceName namespaceName = NamespaceName.get("public", vhost);
        String exchangeTopic = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);
        String queueIndexTopic1 = PersistentQueue.getQueueTopicName(namespaceName, queueName1);
        String queueIndexTopic2 = PersistentQueue.getQueueTopicName(namespaceName, queueName2);

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

    @Test(timeOut = 1000 * 5)
    private void fanoutConsumeTest() throws IOException, TimeoutException, InterruptedException {

        final String vhost = "vhost1";
        final String exchangeName = randExName();
        final String queueName1 = randQuName();
        final String queueName2 = randQuName();

        @Cleanup
        Connection connection = getConnection(vhost, false);
        @Cleanup
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(queueName1, true, false, false, null);
        channel.queueBind(queueName1, exchangeName, "");
        channel.queueDeclare(queueName2, true, false, false, null);
        channel.queueBind(queueName2, exchangeName, "");

        String contentMsg = "Hello AOP!";
        channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());

        CountDownLatch countDownLatch = new CountDownLatch(2);
        final AtomicInteger count = new AtomicInteger(0);

        channel.basicConsume(queueName1, false, "myConsumerTag",
            new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                    Envelope envelope,
                    AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(" [x] Received Consumer '" + message + "'");
                    count.incrementAndGet();
                    countDownLatch.countDown();
                }
            });

        channel.basicConsume(queueName2, false,
            new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                    Envelope envelope,
                    AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(" [x] Received Consumer '" + message + "'");
                    count.incrementAndGet();
                    countDownLatch.countDown();
                }
            });

        countDownLatch.await();
        Assert.assertTrue(count.get() == 2);

    }

    @Test(timeOut = 1000 * 5)
    public void defaultEmptyExchangeTest() throws Exception {
        RabbitMQTestCase rabbitMQTestCase = new RabbitMQTestCase(admin);
        rabbitMQTestCase.defaultEmptyExchangeTest(getAmqpBrokerPortList().get(0), "vhost1");
    }

    @Test(timeOut = 1000 * 20)
    public void basicConsumeCloseAllAndRecreate() throws Exception {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();

        Connection conn = getConnection("vhost1", false);

        int messageCnt = 100;
        CountDownLatch countDownLatch1 = new CountDownLatch(messageCnt);
        AtomicInteger consumeIndex1 = new AtomicInteger(0);

        Channel channel1 = createConsumer(exchangeName, routingKey, queueName, conn, countDownLatch1, consumeIndex1);
        Channel channel2 = createConsumer(exchangeName, routingKey, queueName, conn, countDownLatch1, consumeIndex1);

        for (int i = 0; i < messageCnt; i++) {
            byte[] messageBodyBytes = ("Hello, world! - " + i).getBytes();
            channel1.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }

        countDownLatch1.await();
        Assert.assertEquals(messageCnt, consumeIndex1.get());
        channel1.close();
        channel2.close();

        CountDownLatch countDownLatch2 = new CountDownLatch(messageCnt);
        AtomicInteger consumeIndex2 = new AtomicInteger(0);
        Channel channel3 = createConsumer(exchangeName, routingKey, queueName, conn, countDownLatch2, consumeIndex2);
        for (int i = 0; i <= messageCnt; i++) {
            byte[] messageBodyBytes = ("Hello, world! - " + i).getBytes();
            channel3.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }
        try {
            countDownLatch2.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignored
        }
        Assert.assertEquals(messageCnt, consumeIndex2.get());
        channel3.close();
        conn.close();
    }

    private Channel createConsumer(String exchangeName, String routingKey, String queueName, Connection conn,
                                   CountDownLatch countDownLatch, AtomicInteger consumeIndex) throws IOException {
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        channel.basicConsume(queueName, false,
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        try {
                            consumeIndex.incrementAndGet();
                            long deliveryTag = envelope.getDeliveryTag();
                            // (process the message components here ...)
                            channel.basicAck(deliveryTag, false);
                        } finally {
                            countDownLatch.countDown();
                        }
                    }
                });
        return channel;
    }
}
