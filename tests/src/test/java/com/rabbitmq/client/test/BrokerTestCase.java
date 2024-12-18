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

package com.rabbitmq.client.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.Sets;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownSignalException;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * BrokerTestCase.
 */
@Slf4j
public class BrokerTestCase extends AmqpProtocolHandlerTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTestCase.class);

//    @Rule
//    public TestRule watcher = new TestWatcher() {
//        protected void starting(Description description) {
//            LOGGER.info(
//                "Starting test: {}.{} (nio? {})",
//                description.getTestClass().getSimpleName(), description.getMethodName(), TestUtils.USE_NIO
//            );
//        }
//
//        @Override
//        protected void finished(Description description) {
//            LOGGER.info("Test finished: {}.{}", description.getTestClass().getSimpleName(),
//                    description.getMethodName());
//        }
//    };

    protected ConnectionFactory connectionFactory;

    protected ConnectionFactory newConnectionFactory() {
        ConnectionFactory connectionFactory = TestUtils.connectionFactory();
        connectionFactory.setAutomaticRecoveryEnabled(isAutomaticRecoveryEnabled());
        return connectionFactory;
    }

    protected boolean isAutomaticRecoveryEnabled() {
        return true;
    }

    protected Connection connection;
    protected Channel channel;

    public void setUp() throws Exception{
        this.setup();
    }

    @Override
    @BeforeClass
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

        if (!admin.namespaces().getNamespaces("public").contains("public/vhost1")) {
            admin.namespaces().createNamespace("public/vhost1", 1);
            admin.namespaces().setRetention("public/vhost1",
                    new RetentionPolicies(60, 1000));
        }
        ((PulsarClientImpl) getPulsarServiceList().get(0).getClient()).getLookup().
                getBroker(TopicName.get("public/vhost1/test")).join();
        assumeTrue(shouldRun());
        openConnection();
        openChannel();

        createResources();
    }


    @Override
    @AfterClass
    public void cleanup() throws Exception {
        super.internalCleanup();
//        if(shouldRun()) {
//            closeChannel();
//            closeConnection();
//
//            openConnection();
//            openChannel();
//            releaseResources();
//            closeChannel();
//            closeConnection();
//        }
    }

    @BeforeMethod
    public void openConnectionAndChannel() throws IOException, TimeoutException {
        openConnection();
        openChannel();
    }

    @AfterMethod
    public void closeConnectionAndChannel() throws IOException {
        closeChannel();
        closeConnection();
    }

    /**
     * Whether to run the test or not.
     * Subclasses can check whether some broker features
     * are available or not, and choose not to run the test.
     * @return
     */
    protected boolean shouldRun() throws IOException {
        return true;
    }

    /**
     * Should create any AMQP resources needed by the test. Will be
     * called by BrokerTestCase's implementation of setUp, after the
     * connection and channel have been opened.
     */
    protected void createResources()
            throws IOException, TimeoutException {
    }

    /**
     * Should destroy any AMQP resources that were created by the
     * test. Will be called by BrokerTestCase's implementation of
     * tearDown, after the connection and channel have been closed and
     * reopened specifically for this method. After this method
     * completes, the connection and channel will be closed again.
     */
    protected void releaseResources()
            throws IOException {
    }

    protected void restart()
            throws Exception {
        cleanup();
        bareRestart();
        setup();
    }

    protected void bareRestart()
            throws IOException {
        Host.stopRabbitOnNode();
        Host.startRabbitOnNode();
    }

    public void openConnection()
            throws IOException, TimeoutException {
        if (connection == null) {
            connectionFactory = newConnectionFactory();
            connectionFactory.setPort(getAmqpBrokerPortList().get(0));
            connection = connectionFactory.newConnection();
        }
    }

    public void closeConnection()
            throws IOException {
        if (connection != null) {
            connection.abort();
            connection = null;
        }
    }

    public void openChannel()
            throws IOException {
        channel = connection.createChannel();
    }

    public void closeChannel()
            throws IOException {
        if (channel != null) {
            channel.abort();
            channel = null;
        }
    }

    public void checkShutdownSignal(int expectedCode, IOException ioe) {
        ShutdownSignalException sse = (ShutdownSignalException) ioe.getCause();
        checkShutdownSignal(expectedCode, sse);
    }

    public void checkShutdownSignal(int expectedCode, ShutdownSignalException sse) {
        Method method = sse.getReason();
        channel = null;
        if (sse.isHardError()) {
            AMQP.Connection.Close closeMethod = (AMQP.Connection.Close) method;
            assertEquals(expectedCode, closeMethod.getReplyCode());
        } else {
            AMQP.Channel.Close closeMethod = (AMQP.Channel.Close) method;
            assertEquals(expectedCode, closeMethod.getReplyCode());
        }
    }

    public void expectError(int error) {
        try {
            channel.basicQos(0);
            fail("Expected channel error " + error);
        } catch (IOException ioe) {
            // If we get a channel close back when flushing it with the
            // synchronous basicQos above.
            checkShutdownSignal(error, ioe);
        } catch (AlreadyClosedException ace) {
            // If it has already closed of its own accord before we got there.
            checkShutdownSignal(error, ace);
        }
    }

    protected void assertDelivered(String q, int count) throws IOException {
        assertDelivered(q, count, false);
    }

    protected void assertDelivered(String q, int count, boolean redelivered) throws IOException {
        GetResponse r;
        for (int i = 0; i < count; i++) {
            r = basicGet(q);
            assertNotNull(r);
            assertEquals(redelivered, r.getEnvelope().isRedeliver());
        }
        assertNull(basicGet(q));
    }

    protected GetResponse basicGet(String q) throws IOException {
        return channel.basicGet(q, true);
    }

    protected void basicPublishPersistent(String q) throws IOException {
        basicPublishPersistent("persistent message".getBytes(), q);
    }

    protected void basicPublishPersistent(byte[] msg, String q) throws IOException {
        basicPublishPersistent(msg, "", q);
    }

    protected void basicPublishPersistent(String x, String routingKey) throws IOException {
        basicPublishPersistent("persistent message".getBytes(), x, routingKey);
    }


    protected void basicPublishPersistent(byte[] msg, String x, String routingKey) throws IOException {
        channel.basicPublish(x, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, msg);
    }

    protected void basicPublishVolatile(String q) throws IOException {
        basicPublishVolatile("not persistent message".getBytes(), q);
    }

    protected void basicPublishVolatile(byte[] msg, String q) throws IOException {
        basicPublishVolatile(msg, "", q);
    }

    protected void basicPublishVolatile(String x, String routingKey) throws IOException {
        basicPublishVolatile("not persistent message".getBytes(), x, routingKey);
    }

    protected void basicPublishVolatile(byte[] msg, String x, String routingKey) throws IOException {
        basicPublishVolatile(msg, x, routingKey, MessageProperties.TEXT_PLAIN);
    }

    public void basicPublishVolatile(byte[] msg, String x, String routingKey,
                                        AMQP.BasicProperties properties) throws IOException {
        channel.basicPublish(x, routingKey, properties, msg);
    }

    protected void declareAndBindDurableQueue(String q, String x, String r) throws IOException {
        declareDurableQueue(q);
        channel.queueBind(q, x, r);
    }

    protected void declareExchangeAndQueueToBind(String q, String x, String r) throws IOException {
        declareDurableDirectExchange(x);
        declareAndBindDurableQueue(q, x, r);
    }

    protected void declareDurableDirectExchange(String x) throws IOException {
        channel.exchangeDeclare(x, "direct", true);
    }

    protected void declareDurableQueue(String q) throws IOException {
        channel.queueDeclare(q, true, false, false, null);
    }

    protected void declareTransientQueue(String q) throws IOException {
        channel.queueDeclare(q, false, false, false, null);
    }

    protected void declareTransientQueue(String q, Map<String, Object> args) throws IOException {
        channel.queueDeclare(q, false, false, false, args);
    }

    protected void declareDurableTopicExchange(String x) throws IOException {
        channel.exchangeDeclare(x, "topic", true);
    }

    protected void declareTransientTopicExchange(String x) throws IOException {
        channel.exchangeDeclare(x, "topic", false);
    }

    protected void declareTransientFanoutExchange(String x) throws IOException {
        channel.exchangeDeclare(x, "fanout", false);
    }

    protected void deleteExchange(String x) throws IOException {
        channel.exchangeDelete(x);
    }

    protected void deleteExchanges(String[] exchanges) throws IOException {
        if (exchanges != null) {
            for (String exchange : exchanges) {
                deleteExchange(exchange);
            }
        }
    }

    protected void deleteQueue(String q) throws IOException {
        channel.queueDelete(q);
    }

    protected void deleteQueues(String[] queues) throws IOException {
        if (queues != null) {
            for (String queue : queues) {
                deleteQueue(queue);
            }
        }
    }

    protected void clearAllResourceAlarms() throws IOException {
        clearResourceAlarm("memory");
        clearResourceAlarm("disk");
    }

    protected void setResourceAlarm(String source) throws IOException {
        Host.setResourceAlarm(source);
    }

    protected void clearResourceAlarm(String source) throws IOException {
        Host.clearResourceAlarm(source);
    }

    protected void block() throws IOException, InterruptedException {
        Host.rabbitmqctl("set_vm_memory_high_watermark 0.000000001");
        setResourceAlarm("disk");
    }

    protected void unblock() throws IOException, InterruptedException {
        Host.rabbitmqctl("set_vm_memory_high_watermark 0.4");
        clearResourceAlarm("disk");
    }

    protected String generateQueueName() {
        return "queue" + UUID.randomUUID().toString();
    }

    protected String generateExchangeName() {
        return "exchange" + UUID.randomUUID().toString();
    }

    protected SSLContext getSSLContext() throws NoSuchAlgorithmException {
        return TestUtils.getSSLContext();
    }

    public void publish(int messageCount) throws IOException {
        publish(generateQueueName(), generateExchangeName(), messageCount);
    }

    public void publish(String q, String x, int messageCount) throws IOException {
        declareExchangeAndQueueToBind(q, x, "key1");
        for (int i = 0; i < messageCount; i++) {
            channel.basicPublish(x, "key1", MessageProperties.PERSISTENT_TEXT_PLAIN, "1".getBytes());
        }
    }

}
