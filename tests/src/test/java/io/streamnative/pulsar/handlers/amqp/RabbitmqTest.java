package io.streamnative.pulsar.handlers.amqp;

import com.google.common.collect.Sets;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

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
    private void e2eBasicTest() throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");

        final String QUEUE_NAME = "testQueue";

//        try (Connection connection = connectionFactory.newConnection();
//             Channel channel = connection.createChannel()){
//            CountDownLatch countDownLatch = new CountDownLatch(1);
//            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
//            channel.basicConsume(QUEUE_NAME, (consumeTag, delivery) -> {
//                String message = new String(delivery.getBody());
//                System.out.println("receive: " + message);
//                countDownLatch.countDown();
//            }, consumerTag -> {});
//            countDownLatch.await();
//        }

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            String message = "Hello World!";
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println("send message: " + message);
        }

        Thread.sleep(1000 * 1000);

//        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
//        org.apache.pulsar.client.api.Consumer consumer = pulsarClient.newConsumer()
//                .topic("public/vhost1/" + QUEUE_NAME)
//                .subscriptionName("test")
//                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
//                .subscribe();
//        Message message = consumer.receive();
//        System.out.println("receive msg: " + new String(message.getData()));

    }

}
