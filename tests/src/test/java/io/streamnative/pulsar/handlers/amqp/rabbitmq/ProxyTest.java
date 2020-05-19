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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ProxyTest extends RabbitMQTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        setBrokerCount(3);
        super.setup();
    }

    @Test
    public void test() throws PulsarAdminException, KeeperException, InterruptedException {
        getPulsarServiceList().get(0).getLocalZkCache().getZooKeeper().getData("/namespace/public/vhost1/0x00000000_0xffffffff", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                admin.lookups().lookupTopicAsync("persistent://public/vhost1/__lookup__");
            }
        }, null);
        admin.namespaces().unload("public/vhost1");
    }

    @Test
    public void unloadBundleTest() throws Exception {

        @Cleanup
        Connection connection = getConnection("vhost1", true);
        @Cleanup
        Channel channel = connection.createChannel();

        String exchangeName = "ex1";
        String queueName = "ex1-q1";
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, "");

        final String msgContent = "Hello AOP!";

        new Thread(() -> {
            while (true) {
                try {
                    channel.basicPublish(exchangeName, "", null, msgContent.getBytes());
                    Thread.sleep(100);
                } catch (Exception e) {
//                    log.warn("produce message failed.");
                }
            }
        }).start();

        new Thread(() -> {
            try {
                log.info("unload ns start.");
                admin.namespaces().unload("public/vhost1");
                log.info("unload ns finish.");
            } catch (Exception e) {
                log.error("unload failed ns: {}", "public/vhost1", e);
            }
        }).start();

        AtomicInteger receiveMsgCnt = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(100);
        new Thread(() -> {
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println(message);
                    Assert.assertEquals(message, msgContent);
                    synchronized (countDownLatch) {
                        countDownLatch.countDown();
                        receiveMsgCnt.getAndIncrement();
                    }
                }
            };
            try {
                channel.basicConsume(queueName, false, consumer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        countDownLatch.await();
        Assert.assertEquals(receiveMsgCnt.get(), 100);
    }

    @Test
    public void proxyBasicTest() throws Exception {

        fanoutTest("test1", "vhost1", "ex1", Arrays.asList("ex1-q1", "ex1-q2"));
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
