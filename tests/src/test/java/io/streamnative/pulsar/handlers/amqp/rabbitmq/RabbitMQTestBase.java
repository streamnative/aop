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

import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for RabbitMQ Client.
 */
@Slf4j
public class RabbitMQTestBase extends AmqpProtocolHandlerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + getBrokerWebservicePortList().get(0)));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + getBrokerWebServicePortTlsList().get(0)));
        }
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }

        List<String> vhostList = Arrays.asList("vhost1", "vhost2", "vhost3");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns);
                admin.namespaces().setRetention(ns,
                        new RetentionPolicies(60, 1000));
            }
        }
        checkPulsarServiceState();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected Connection getConnection(String vhost, boolean useProxy) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        if (useProxy) {
            int proxyPort = getProxyPort();
            connectionFactory.setPort(proxyPort);
            log.info("use proxyPort: {}", proxyPort);
        } else {
            connectionFactory.setPort(getAmqpBrokerPortList().get(0));
            log.info("use amqpBrokerPort: {}", getAmqpBrokerPortList().get(0));
        }
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

    public void basicFanoutTest(String testName, String vhost, boolean bundleUnloadTest,
                                int queueCnt) throws Exception {
        log.info("[{}] test start ...", testName);
        @Cleanup
        Connection connection = getConnection(vhost, true);
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
                            sendMsgCnt.addAndGet(1);
                        } else if (isBundleUnload.get()) {
                            // send message until consumer get enough messages
                            // TODO If add send confirm, only send expected messages is enough
                            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
                            sendMsgCnt.addAndGet(1);
                            Thread.sleep(10);
                        }
                    } else {
                        // bundle not change
                        if (sendMsgCnt.get() < expectedMsgCntPerQueue) {
                            channel.basicPublish(exchangeName, "", null, contentMsg.getBytes());
                            sendMsgCnt.addAndGet(1);
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
                    synchronized (countDownLatch) {
                        countDownLatch.countDown();
                    }
                    totalReceiveMsgCnt.addAndGet(1);
                }
            };
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
                admin.namespaces().unloadAsync("public/" + vhost).whenComplete((ignore, throwable) -> {
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
}
