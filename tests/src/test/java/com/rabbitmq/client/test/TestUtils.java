
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

import static org.junit.Assert.assertTrue;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.impl.NetworkConnection;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import java.io.IOException;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import javax.net.ssl.SSLContext;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.LoggerFactory;

/**
 * TestUtils.
 */
public class TestUtils {

    public static final boolean USE_NIO = System.getProperty("use.nio") != null;

    public static ConnectionFactory connectionFactory() {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);
        connectionFactory.setVirtualHost("vhost1");
        if (USE_NIO) {
            connectionFactory.useNio();
        } else {
            connectionFactory.useBlockingIo();
        }
        return connectionFactory;
    }

    public static void waitAtMost(Duration timeout, BooleanSupplier condition) {
        if (condition.getAsBoolean()) {
            return;
        }
        int waitTime = 100;
        int waitedTime = 0;
        long timeoutInMs = timeout.toMillis();
        while (waitedTime <= timeoutInMs) {
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            if (condition.getAsBoolean()) {
                return;
            }
            waitedTime += waitTime;
        }
        Assert.fail("Waited " + timeout.getSeconds() + " second(s), condition never got true");
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void abort(Connection connection) {
        if (connection != null) {
            connection.abort();
        }
    }

    public static SSLContext getSSLContext() throws NoSuchAlgorithmException {
        SSLContext c = null;

        // pick the first protocol available, preferring TLSv1.2, then TLSv1,
        // falling back to SSLv3 if running on an ancient/crippled JDK
        for (String proto : Arrays.asList("TLSv1.2", "TLSv1", "SSLv3")) {
            try {
                c = SSLContext.getInstance(proto);
                return c;
            } catch (NoSuchAlgorithmException x) {
                // keep trying
            }
        }
        throw new NoSuchAlgorithmException();
    }

    public static TestRule atLeast38() {
        return new BrokerVersionTestRule("3.8.0");
    }

    public static boolean isVersion37orLater(Connection connection) {
        return atLeastVersion("3.7.0", connection);
    }

    public static boolean isVersion38orLater(Connection connection) {
        return atLeastVersion("3.8.0", connection);
    }

    private static boolean atLeastVersion(String expectedVersion, Connection connection) {
        String currentVersion = null;
        try {
            currentVersion = currentVersion(
                    connection.getServerProperties().get("version").toString()
            );
            return "0.0.0".equals(currentVersion) || versionCompare(currentVersion, expectedVersion) >= 0;
        } catch (RuntimeException e) {
            LoggerFactory.getLogger(TestUtils.class).warn("Unable to parse broker version {}", currentVersion, e);
            throw e;
        }
    }

    private static String currentVersion(String currentVersion) {
        // versions built from source: 3.7.0+rc.1.4.gedc5d96
        if (currentVersion.contains("+")) {
            currentVersion = currentVersion.substring(0, currentVersion.indexOf("+"));
        }
        // alpha (snapshot) versions: 3.7.0~alpha.449-1
        if (currentVersion.contains("~")) {
            currentVersion = currentVersion.substring(0, currentVersion.indexOf("~"));
        }
        // alpha (snapshot) versions: 3.7.1-alpha.40
        if (currentVersion.contains("-")) {
            currentVersion = currentVersion.substring(0, currentVersion.indexOf("-"));
        }
        return currentVersion;
    }

    public static boolean sendAndConsumeMessage(String exchange, String routingKey, String queue, Connection c)
            throws IOException, TimeoutException, InterruptedException {
        Channel ch = c.createChannel();
        try {
            ch.confirmSelect();
            final CountDownLatch latch = new CountDownLatch(1);
            ch.basicConsume(queue, true, new DefaultConsumer(ch) {

                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    latch.countDown();
                }
            });
            ch.basicPublish(exchange, routingKey, null, "".getBytes());
            ch.waitForConfirmsOrDie(5000);
            return latch.await(5, TimeUnit.SECONDS);
        } finally {
            if (ch != null && ch.isOpen()) {
                ch.close();
            }
        }
    }

    public static boolean resourceExists(Callable<Channel> callback) throws Exception {
        Channel declarePassiveChannel = null;
        try {
            declarePassiveChannel = callback.call();
            return true;
        } catch (IOException e) {
            if (e.getCause() instanceof ShutdownSignalException) {
                ShutdownSignalException cause = (ShutdownSignalException) e.getCause();
                if (cause.getReason() instanceof AMQP.Channel.Close) {
                    if (((AMQP.Channel.Close) cause.getReason()).getReplyCode() == 404) {
                        return false;
                    } else {
                        throw e;
                    }
                }
                return false;
            } else {
                throw e;
            }
        } finally {
            if (declarePassiveChannel != null && declarePassiveChannel.isOpen()) {
                declarePassiveChannel.close();
            }
        }
    }

    public static boolean queueExists(final String queue, final Connection connection) throws Exception {
        return resourceExists(() -> {
            Channel channel = connection.createChannel();
            channel.queueDeclarePassive(queue);
            return channel;
        });
    }

    public static boolean exchangeExists(final String exchange, final Connection connection) throws Exception {
        return resourceExists(() -> {
            Channel channel = connection.createChannel();
            channel.exchangeDeclarePassive(exchange);
            return channel;
        });
    }

    public static void closeAndWaitForRecovery(RecoverableConnection connection)
            throws IOException, InterruptedException {
        CountDownLatch latch = prepareForRecovery(connection);
        Host.closeConnection((NetworkConnection) connection);
        wait(latch);
    }

    public static void closeAllConnectionsAndWaitForRecovery(Collection<Connection> connections)
            throws IOException, InterruptedException {
        CountDownLatch latch = prepareForRecovery(connections);
        Host.closeAllConnections();
        wait(latch);
    }

    public static void closeAllConnectionsAndWaitForRecovery(Connection connection)
            throws IOException, InterruptedException {
        closeAllConnectionsAndWaitForRecovery(Collections.singletonList(connection));
    }

    public static CountDownLatch prepareForRecovery(Connection connection) {
        return prepareForRecovery(Collections.singletonList(connection));
    }

    public static CountDownLatch prepareForRecovery(Collection<Connection> connections) {
        final CountDownLatch latch = new CountDownLatch(connections.size());
        for (Connection conn : connections) {
            ((AutorecoveringConnection) conn).addRecoveryListener(new RecoveryListener() {

                @Override
                public void handleRecovery(Recoverable recoverable) {
                    latch.countDown();
                }

                @Override
                public void handleRecoveryStarted(Recoverable recoverable) {
                    // No-op
                }
            });
        }
        return latch;
    }

    private static void wait(CountDownLatch latch) throws InterruptedException {
        assertTrue(latch.await(90, TimeUnit.SECONDS));
    }

    /**
     * https://stackoverflow.com/questions/6701948/efficient-way-to-compare-version-strings-in-java.
     */
    static int versionCompare(String str1, String str2) {
        String[] vals1 = str1.split("\\.");
        String[] vals2 = str2.split("\\.");
        int i = 0;
        // set index to first non-equal ordinal or length of shortest version string
        while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
            i++;
        }
        // compare first non-equal ordinal number
        if (i < vals1.length && i < vals2.length) {
            int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
            return Integer.signum(diff);
        }
        // the strings are equal or one string is a substring of the other
        // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
        return Integer.signum(vals1.length - vals2.length);
    }

    public static int randomNetworkPort() throws IOException {
        ServerSocket socket = new ServerSocket();
        socket.bind(null);
        int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    private static class BrokerVersionTestRule implements TestRule {

        private final String version;

        public BrokerVersionTestRule(String version) {
            this.version = version;
        }

        @Override
        public Statement apply(Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    try (Connection c = TestUtils.connectionFactory().newConnection()) {
                        if (!TestUtils.atLeastVersion(version, c)) {
                            throw new AssumptionViolatedException("Broker version < " + version + ", skipping.");
                        }
                    }
                    base.evaluate();
                }
            };
        }
    }
}
