package io.streamnative.pulsar.handlers.amqp.integration;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterTestBase;
import lombok.Cleanup;
import org.testng.Assert;
import org.testng.ITest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

public class SmockTest extends PulsarClusterTestSuite implements ITest {

    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("amqpProxyEnable", "true");
        return result;
    }

    @Test(timeOut = 1000 * 5)
    public void basic_consume_case() throws IOException, TimeoutException, InterruptedException {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();
        @Cleanup
        Connection conn = getConnection("vhost1", false);
        @Cleanup
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        List<String> messages = new ArrayList<>();
        channel.basicConsume(queueName, false,
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
                        countDownLatch.countDown();
                    }
                });

        byte[] messageBodyBytes = "Hello, world!".getBytes();
        channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);

        countDownLatch.await();
        Assert.assertEquals(messages.get(0), "Hello, world!");
    }
    @Override
    public String getTestName() {
        return "Smock test";
    }
}
