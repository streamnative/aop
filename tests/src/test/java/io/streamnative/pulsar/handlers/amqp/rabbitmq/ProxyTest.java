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
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProxyTest extends RabbitMQTestBase {

    @Test
    public void proxyBasicTest() throws Exception {

        fanoutTest("test2", "vhost2", "ex2", Arrays.asList("ex2-q1", "ex2-q2"));
        fanoutTest("test3", "vhost3", "ex3", Arrays.asList("ex3-q1", "ex3-q2"));

        CountDownLatch countDownLatch = new CountDownLatch(3);
        new Thread(() -> {
            try {
                fanoutTest("test4", "vhost1", "ex4", Arrays.asList("ex4-q1", "ex4-q2", "ex4-q3"));
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test4 error for vhost1.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                fanoutTest("test5", "vhost2", "ex5", Arrays.asList("ex5-q1", "ex5-q2"));
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test5 error for vhost2.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                fanoutTest("test6", "vhost3", "ex6", Arrays.asList("ex6-q1", "ex6-q2", "ex6-q3"));
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test6 error for vhost3.", e);
            }
        }).start();
        countDownLatch.await();

//        admin.namespaces().unload("public/vhost1");
//        admin.namespaces().unload("public/vhost2");
//        admin.namespaces().unload("public/vhost3");
//
//        Thread.sleep(1000 * 2);
//
//        log.info("unload namespaces finish");
//
//        fanoutTest("test7", "vhost1", "ex7", Arrays.asList("ex7-q1", "ex7-q2"));
//        fanoutTest("test8", "vhost2", "ex8", Arrays.asList("ex8-q1", "ex8-q2"));
//        fanoutTest("test9", "vhost3", "ex9", Arrays.asList("ex9-q1", "ex9-q2"));
    }

    private void fanoutTest(String testName, String vhost, String exchangeName, List<String> queueList) throws Exception {
        log.info("[{}] test start ...", testName);
        @Cleanup
        Connection connection = getConnection(vhost, true);
        log.info("[{}] connection init finish. address: {}:{} open: {}",
                testName, connection.getAddress(), connection.getPort(), connection.isOpen());
        @Cleanup
        Channel channel = connection.createChannel();
        log.info("[{}] channel init finish. channelNum: {}", testName, channel.getChannelNumber());

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
        log.info("[{}] send msg finish. msgCnt: {}", testName, msgCnt);

        AtomicInteger totalMsgCnt = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(msgCnt * queueList.size());

        for (String queueName : queueList) {
            Channel consumeChannel = connection.createChannel();
            log.info("[{}] consumeChannel init finish. channelNum: {}", testName, consumeChannel.getChannelNumber());
            Consumer consumer = new DefaultConsumer(consumeChannel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    Assert.assertEquals(message, contentMsg);
                    synchronized (countDownLatch) {
                        countDownLatch.countDown();
                    }
                    totalMsgCnt.addAndGet(1);
                }
            };
            consumeChannel.basicConsume(queueName, false, consumer);
            log.info("[{}] consume start. queueName: {}", testName, queueName);
        }

        countDownLatch.await();
        System.out.println("[" + testName + "] Total msg cnt: " + totalMsgCnt);
        Assert.assertEquals(msgCnt * queueList.size(), totalMsgCnt.get());
    }

}
