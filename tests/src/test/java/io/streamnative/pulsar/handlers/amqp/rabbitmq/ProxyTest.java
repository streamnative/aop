package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.proxy.ProxyException;
import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProxyTest extends RabbitMQTestBase {

    @Test
    public void proxyTest() throws InterruptedException, TimeoutException, IOException, ProxyException {

        fanoutTest("vhost1", "ex1", Arrays.asList("ex1-q1", "ex1-q2"));
        fanoutTest("vhost2", "ex2", Arrays.asList("ex2-q1", "ex2-q2"));
        fanoutTest("vhost3", "ex3", Arrays.asList("ex3-q1", "ex3-q2"));

        CountDownLatch countDownLatch = new CountDownLatch(3);
        new Thread(() -> {
            try {
                fanoutTest("vhost1", "ex4", Arrays.asList("ex4-q1", "ex4-q2", "ex4-q3"));
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                fanoutTest("vhost2", "ex5", Arrays.asList("ex5-q1", "ex5-q2"));
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        new Thread(() -> {
            try {
                fanoutTest("vhost3", "ex6", Arrays.asList("ex6-q1", "ex6-q2", "ex6-q3"));
                countDownLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
        countDownLatch.await();
    }

    private void fanoutTest(String vhost, String exchangeName, List<String> queueList) throws IOException, TimeoutException, InterruptedException, ProxyException {

        @Cleanup
        Connection connection = getConnection(vhost, true);
        log.info("connection init finish. address: {}:{} open: {}",
                connection.getAddress(), connection.getPort(), connection.isOpen());
        @Cleanup
        Channel channel = connection.createChannel();
        log.info("channel init finish. channelNum: {}", channel.getChannelNumber());

        PulsarServiceLookupHandler lookupHandler = new PulsarServiceLookupHandler(getPulsarServiceList().get(0));
        Pair<String, Integer> lookupData = lookupHandler.findBroker(NamespaceName.get("public/" + vhost), AmqpProtocolHandler.PROTOCOL_NAME);
        log.info("Find broker namespaceName: {}, hostname: {}, port: {}", "public/vhost1", lookupData.getLeft(), lookupData.getRight());

        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);

        for (String queueName : queueList) {
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchangeName, "");
        }

        String contentMsg = "Hello AOP!";
        int msgCnt = 100;
        for (int i = 0; i < msgCnt; i++) {
            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
        }
        log.info("send msg finish. msgCnt: {}", msgCnt);

        AtomicInteger totalMsgCnt = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(msgCnt * queueList.size());

        for (String queueName : queueList) {
            Channel consumeChannel = connection.createChannel();
            log.info("consumeChannel init finish. channelNum: {}", consumeChannel.getChannelNumber());
            Consumer consumer = new DefaultConsumer(consumeChannel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    Assert.assertEquals(message, contentMsg);
                    countDownLatch.countDown();
                    totalMsgCnt.addAndGet(1);
                }
            };
            consumeChannel.basicConsume(queueName, false, consumer);
            log.info("consume start. queueName: {}", queueName);
        }

        countDownLatch.await();
        System.out.println("Total msg cnt: " + totalMsgCnt);
        Assert.assertEquals(msgCnt * queueList.size(), totalMsgCnt.get());
    }

}
