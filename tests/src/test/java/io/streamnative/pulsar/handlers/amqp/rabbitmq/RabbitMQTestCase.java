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

import static io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase.randExName;
import static io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase.randQuName;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.testng.Assert;

/**
 * RabbitMQ test case.
 */
@Slf4j
public class RabbitMQTestCase {

    private PulsarAdmin pulsarAdmin;

    public RabbitMQTestCase(PulsarAdmin pulsarAdmin) {
        this.pulsarAdmin = pulsarAdmin;
    }

    public void basicFanoutTest(int aopPort, String testName, String vhost, boolean bundleUnloadTest,
                                int queueCnt) throws Exception {
        log.info("[{}] test start ...", testName);
        @Cleanup
        Connection connection = getConnection(vhost, aopPort);
        log.info("[{}] connection init finish. address: {}:{} open: {}",
                testName, connection.getAddress(), connection.getPort(), connection.isOpen());
        @Cleanup
        Channel channel = connection.createChannel();
        log.info("[{}] channel init finish. channelNum: {}", testName, channel.getChannelNumber());

        String exchangeName = randExName();
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);

        List<String> queueList = new ArrayList<>();
        for (int i = 0; i < queueCnt; i++) {
            String queueName = randQuName();
            queueList.add(queueName);
            channel.queueDeclare(queueName, true, false, false, null);
            channel.queueBind(queueName, exchangeName, "");
        }

        String contentMsg = "Hello AOP!";
        AtomicBoolean isBundleUnload = new AtomicBoolean(false);
        AtomicInteger sendMsgCnt = new AtomicInteger(0);
        int expectedMsgCntPerQueue = 100;
        log.info("[{}] send msg start.", testName);
        new Thread(() -> {
            while (true) {
                try {
                    // bundle will be unloaded
                    if (bundleUnloadTest) {
                        if (!isBundleUnload.get() && sendMsgCnt.get() < (expectedMsgCntPerQueue / 2)) {
                            // if bundle will be unloaded, we send half of `expectedMsgCntPerQueue`
                            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
                            sendMsgCnt.incrementAndGet();
                        } else if (isBundleUnload.get()) {
                            // send message until consumer get enough messages
                            // Add send confirm, only send expected messages is enough
                            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
                            sendMsgCnt.incrementAndGet();
                            Thread.sleep(10);
                        }
                    } else {
                        // bundle not change
                        if (sendMsgCnt.get() < expectedMsgCntPerQueue) {
                            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
                            sendMsgCnt.incrementAndGet();
                        } else {
                            log.info("message send finish. produce msg cnt: {}", sendMsgCnt.get());
                            break;
                        }
                    }
                } catch (Exception e) {
                    if (!bundleUnloadTest) {
                        Assert.fail("[" + testName + "] Failed to send message. exchangeName: " + exchangeName, e);
                    }
                }
            }
        }).start();

        AtomicInteger totalReceiveMsgCnt = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(expectedMsgCntPerQueue * queueList.size());

        for (String queueName : queueList) {
            Channel consumeChannel = connection.createChannel();
            log.info("[{}] consumeChannel init finish. channelNum: {}", testName, consumeChannel.getChannelNumber());
            Consumer consumer = new DefaultConsumer(consumeChannel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    Assert.assertEquals(message, contentMsg);
                    if (bundleUnloadTest && totalReceiveMsgCnt.get() == expectedMsgCntPerQueue * queueList.size()) {
                        // If test is bundleUnloadTest, stop totalReceiveMsgCnt
                        // when totalReceiveMsgCnt reach the expectedCount
                        return;
                    }
                    totalReceiveMsgCnt.incrementAndGet();
                    countDownLatch.countDown();
                }
            };
            consumeChannel.setDefaultConsumer(consumer);
            try {
                consumeChannel.basicConsume(queueName, false, consumer);
                log.info("[{}] consume start. queueName: {}", testName, queueName);
            } catch (Exception e) {
                Assert.fail("[" + testName + "] Failed to start consume. queueName: " + queueName, e);
            }
        }

        if (bundleUnloadTest) {
            try {
                log.info("unload bundle start.");
                pulsarAdmin.namespaces().unloadAsync("public/" + vhost).whenComplete((ignore, throwable) -> {
                    isBundleUnload.set(true);
                    log.info("unload bundle finish.");
                });
            } catch (Exception e) {
                Assert.fail("[" + testName + "] Failed to unload bundle. vhost: " + vhost, e);
            }
        }

        countDownLatch.await();
        System.out.println("[" + testName + "] Test finish. Receive total msg cnt: " + totalReceiveMsgCnt);
        Assert.assertEquals(expectedMsgCntPerQueue * queueList.size(), totalReceiveMsgCnt.get());
    }

    public void defaultEmptyExchangeTest(int aopPort, String vhost) throws Exception {
        @Cleanup
        Connection connection = getConnection(vhost, aopPort);
        @Cleanup
        Channel channel = connection.createChannel();

        final int queueCount = 5;
        final int messageCount = 100;
        final String contentMsg = "Hello AoP ";

        List<String> queueNameList = new ArrayList<>();
        for (int i = 0; i < queueCount; i++) {
            String queueName = randQuName();
            queueNameList.add(queueName);
            channel.queueDeclare(queueName, true, false, false, null);
        }

        for (String queueName : queueNameList) {
            for (int i = 0; i < messageCount; i++) {
                channel.basicPublish("", queueName, null, (contentMsg + i).getBytes(UTF_8));
            }
        }

        AtomicInteger receiveMsgCount = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(messageCount * queueCount);
        for (String queueName : queueNameList) {
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    receiveMsgCount.incrementAndGet();
                    countDownLatch.countDown();
                }
            };
            channel.setDefaultConsumer(consumer);
            channel.basicConsume(queueName, consumer);
        }
        countDownLatch.await();
        Assert.assertEquals(messageCount * queueCount, receiveMsgCount.get());
    }

    private Connection getConnection(String vhost, int port) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

}
