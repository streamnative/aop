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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * RabbitMQ client test.
 */
@Slf4j
public class RabbitmqTest extends AmqpProtocolHandlerTestBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + brokerWebservicePort));
        }
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }

        if (!admin.namespaces().getNamespaces("public").contains("public/vhost1")) {
            admin.namespaces().createNamespace("public/vhost1");
            admin.namespaces().setRetention("public/vhost1",
                    new RetentionPolicies(60, 1000));
        }
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    private void e2eBasicTest() throws IOException, TimeoutException, InterruptedException, PulsarAdminException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");

        final String queueName = "testQueue";
        final String message = "Hello AOP!";
        final int messagesNum = 10;

        @Cleanup
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl("http://127.0.0.1:"
                + brokerWebservicePort).build();
        log.info("topics: {}", pulsarAdmin.topics());

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);
            for (int i = 0; i < messagesNum; i++) {
                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println("send message: " + message);
            }
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl("pulsar://localhost:" + brokerPort).build();
        @Cleanup
        org.apache.pulsar.client.api.Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic("persistent://public/vhost1/" + queueName)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        for (int i = 0; i < messagesNum; i++) {
            Message<byte[]> msg = consumer.receive();
            System.out.println("receive msg: " + new String(msg.getData()));
            Assert.assertEquals(new String(msg.getData()), message);
        }
    }

}
