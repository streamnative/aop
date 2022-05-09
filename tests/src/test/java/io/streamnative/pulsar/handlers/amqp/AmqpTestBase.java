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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for RabbitMQ Client.
 */
@Slf4j
public class AmqpTestBase extends AmqpProtocolHandlerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        ClusterData clusterData = ClusterData.builder()
                .serviceUrl("http://127.0.0.1:" + getBrokerWebservicePortList().get(0))
                .build();
        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName, clusterData);
        } else {
            admin.clusters().updateCluster(configClusterName, clusterData);
        }

        TenantInfo tenantInfo = TenantInfo.builder()
                .adminRoles(Sets.newHashSet("appid1", "appid2"))
                .allowedClusters(Sets.newHashSet("test"))
                .build();
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public", tenantInfo);
        } else {
            admin.tenants().updateTenant("public", tenantInfo);
        }

        List<String> vhostList = Arrays.asList("vhost1", "vhost2", "vhost3");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns, 1);
                admin.lookups().lookupTopicAsync(TopicName.get(TopicDomain.persistent.value(),
                        NamespaceName.get(ns), "__lookup__").toString());
            }
        }

        checkPulsarServiceState();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected Connection getConnection(String vhost, boolean amqpProxyEnable) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        if (amqpProxyEnable) {
            int proxyPort = getProxyPort();
            log.info("use proxyPort: {}", proxyPort);
        } else {
            connectionFactory.setPort(getAmqpBrokerPortList().get(0));
            log.info("use amqpBrokerPort: {}", getAmqpBrokerPortList().get(0));
        }
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

    protected void basicDirectConsume(String vhost) throws Exception {
        String exchangeName = randExName();
        String routingKey = "test.key";
        String queueName = randQuName();

        Connection conn = getConnection(vhost, false);
        Channel channel = conn.createChannel();

        channel.exchangeDeclare(exchangeName, "direct", true);
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);

        int messageCnt = 100;
        CountDownLatch countDownLatch = new CountDownLatch(messageCnt);

        AtomicInteger consumeIndex = new AtomicInteger(0);
        channel.basicConsume(queueName, false,
                new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        long deliveryTag = envelope.getDeliveryTag();
                        Assert.assertEquals(new String(body), "Hello, world! - " + consumeIndex.getAndIncrement());
                        // (process the message components here ...)
                        channel.basicAck(deliveryTag, false);
                        countDownLatch.countDown();
                    }
                });

        for (int i = 0; i < messageCnt; i++) {
            byte[] messageBodyBytes = ("Hello, world! - " + i).getBytes();
            channel.basicPublish(exchangeName, routingKey, null, messageBodyBytes);
        }

        countDownLatch.await();
        Assert.assertEquals(messageCnt, consumeIndex.get());
        channel.close();
        conn.close();
    }

}
