package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class ResourceReleaseTest extends RabbitMQTestBase {

    @Test
    public void bundleUnloadTest() throws IOException, TimeoutException, PulsarAdminException, ExecutionException, InterruptedException {

        @Cleanup
        Connection connection = getConnection();
        @Cleanup
        Channel channel = connection.createChannel();

        String msgContent = "Hello AOP.";
        String exchange = "ex1";
        String queue = "qu1";
        channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, "");

        int msgCnt = 100;
        for (int i = 0; i < msgCnt; i++) {
            channel.basicPublish(exchange, "", null, msgContent.getBytes());
        }

        CountDownLatch consumeLatch = new CountDownLatch(msgCnt);
        Channel consumeChannel = connection.createChannel();
        Consumer consumer = new DefaultConsumer(consumeChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                synchronized (consumeLatch) {
                    consumeLatch.countDown();
                }
                Assert.assertEquals(msgContent, new String(body));
            }
        };
        consumeChannel.basicConsume(queue, consumer);
        consumeLatch.await();

        try {
            admin.namespaces().unload("public/vhost1");
            log.info("unload namespace public/vhost1.");
        } catch (Exception e) {
            Assert.fail("Unload namespace public/vhost1 failed. errorMsg: " + e.getMessage());
        }

        Assert.assertFalse(connection.isOpen());
        Assert.assertFalse(channel.isOpen());
        Assert.assertFalse(consumeChannel.isOpen());

        connection = getConnection();
        channel = connection.createChannel();

        msgCnt = 100;
        for (int i = 0; i < msgCnt; i++) {
            channel.basicPublish(exchange, "", null, msgContent.getBytes());
        }

        CountDownLatch consumeLatch2 = new CountDownLatch(msgCnt);
        consumeChannel = connection.createChannel();
        Consumer consumer2 = new DefaultConsumer(consumeChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                synchronized (consumeLatch2) {
                    consumeLatch2.countDown();
                }
                Assert.assertEquals(msgContent, new String(body));
            }
        };
        consumeChannel.basicConsume(queue, consumer);
        consumeLatch2.await();

        log.info("bundleUnloadTest finish.");
    }

}
