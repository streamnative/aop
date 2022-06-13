package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import static org.awaitility.Awaitility.await;
import static org.testng.AssertJUnit.fail;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.streamnative.pulsar.handlers.amqp.AmqpTestBase;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

@Slf4j
public class BrokerRestartTest extends AmqpTestBase {

    @Test(timeOut = 1000 * 30)
    public void basicProduceAndConsumeTest() throws Exception {
        String vhost = "vhost1";
        String exchange = randExName();
        String queue = randQuName();
        String queue2 = randQuName();
        String routingKey = "key";

        Connection connection = getConnection(vhost, false);
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        channel.exchangeDeclare(exchange, BuiltinExchangeType.DIRECT.getType(), true);

        channel.queueDeclare(queue, true, true, false, null);
        channel.queueBind(queue, exchange, routingKey);

        channel.queueDeclare(queue2, true, true, false, null);
        channel.queueBind(queue2, exchange, routingKey);

        int count = 1000;
        Set<String> messageSet1 = new HashSet<>();
        Set<String> messageSet2 = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String message = "msg-" + i;
            channel.basicPublish(exchange, routingKey, null, message.getBytes());
            messageSet1.add(message);
            messageSet2.add(message);
        }
        if (!channel.waitForConfirms(5000)) {
            fail("Failed to publish messages.");
        }
        channel.close();
        connection.close();
        // Currently, some messages in exchange may not route to queue topic.
        // Make sure all messages route to queue topic after broker restart.
        restartBroker();

        connection = getConnection(vhost, false);
        channel = connection.createChannel();

        channel.basicConsume(queue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                System.out.println("1 receive msg " + msg);
                messageSet1.remove(msg);
            }
        });

        channel.basicConsume(queue2, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                System.out.println("2 receive msg " + msg);
                messageSet2.remove(msg);
            }
        });

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    log.info("check queue1 {} set1: {}, queue2 {} set2: {}",
                            queue, messageSet1.size(), queue2, messageSet2.size());
                    log.info("queue1 set: {}", messageSet1);
                    log.info("queue2 set: {}", messageSet2);
                    return messageSet1.size() == 0 && messageSet2.size() == 0;
                });

        channel.close();
        connection.close();
    }

}
