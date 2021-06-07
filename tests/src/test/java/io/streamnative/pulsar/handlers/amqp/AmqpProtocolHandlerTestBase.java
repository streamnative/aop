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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import io.netty.channel.EventLoopGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.ZooKeeperClientFactory;
import org.apache.pulsar.zookeeper.ZookeeperClientFactoryImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.mockito.Mockito;

/**
 * Unit test to test AoP handler.
 */
@Slf4j
public abstract class AmqpProtocolHandlerTestBase {

    protected ServiceConfiguration conf;
    protected PulsarAdmin admin;
    protected URL brokerUrl;
    protected URL brokerUrlTls;
    protected URI lookupUrl;
    protected PulsarClient pulsarClient;

    @Getter
    protected int amqpBrokerPortTls = PortManager.nextFreePort();

    protected MockZooKeeper mockZooKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;
    protected boolean isTcpLookup = false;
    protected final String configClusterName = "test";

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private OrderedExecutor bkExecutor;

    private int brokerCount = 1;
    @Getter
    private List<PulsarService> pulsarServiceList = new ArrayList<>();
    @Getter
    private List<Integer > brokerPortList = new ArrayList<>();
    @Getter
    private List<Integer> brokerWebservicePortList = new ArrayList<>();
    @Getter
    private List<Integer> brokerWebServicePortTlsList = new ArrayList<>();
    @Getter
    private List<Integer> amqpBrokerPortList = new ArrayList<>();
    @Getter
    private List<Integer> aopProxyPortList = new ArrayList<>();

    public AmqpProtocolHandlerTestBase() {
        resetConfig();
    }

    protected void resetConfig() {
        AmqpServiceConfiguration amqpConfig = new AmqpServiceConfiguration();
        amqpConfig.setAdvertisedAddress("localhost");
        amqpConfig.setClusterName(configClusterName);

        amqpConfig.setManagedLedgerCacheSizeMB(8);
        amqpConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        amqpConfig.setDefaultRetentionTimeInMinutes(7);
        amqpConfig.setDefaultNumberOfNamespaceBundles(1);
        amqpConfig.setZookeeperServers("localhost:2181");
        amqpConfig.setConfigurationStoreServers("localhost:3181");

        amqpConfig.setAuthenticationEnabled(false);
        amqpConfig.setAuthorizationEnabled(false);
        amqpConfig.setAllowAutoTopicCreation(true);
        amqpConfig.setAllowAutoTopicCreationType("partitioned");
        amqpConfig.setBrokerDeleteInactiveTopicsEnabled(false);

        // set protocol related config
        URL testHandlerUrl = this.getClass().getClassLoader().getResource("test-protocol-handler.nar");
        Path handlerPath;
        try {
            handlerPath = Paths.get(testHandlerUrl.toURI());
        } catch (Exception e) {
            log.error("failed to get handler Path, handlerUrl: {}. Exception: ", testHandlerUrl, e);
            return;
        }

        String protocolHandlerDir = handlerPath.toFile().getParent();

        amqpConfig.setProtocolHandlerDirectory(
            protocolHandlerDir
        );
        amqpConfig.setMessagingProtocols(Sets.newHashSet("amqp"));

        this.conf = amqpConfig;
    }

    protected final void internalSetup() throws Exception {
        init();
        lookupUrl = new URI(brokerUrl.toString());
        if (isTcpLookup) {
            lookupUrl = new URI("broker://localhost:" + brokerPortList.get(0));
        }
        pulsarClient = newPulsarClient(lookupUrl.toString(), 0);
    }

    protected PulsarClient newPulsarClient(String url, int intervalInSecs) throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(url).statsInterval(intervalInSecs, TimeUnit.SECONDS).build();
    }

    protected final void init() throws Exception {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = OrderedExecutor.newBuilder().numThreads(1).name("mock-pulsar-bk").build();

        mockZooKeeper = createMockZooKeeper();
        mockBookKeeper = createMockBookKeeper(bkExecutor);

        startBroker();

        brokerUrl = new URL("http://" + pulsarServiceList.get(0).getAdvertisedAddress() + ":"
                + pulsarServiceList.get(0).getConfiguration().getWebServicePort().get());
        brokerUrlTls = new URL("https://" + pulsarServiceList.get(0).getAdvertisedAddress() + ":"
                + pulsarServiceList.get(0).getConfiguration().getWebServicePortTls().get());

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(brokerUrl.toString()).build());
    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
            }
            if (pulsarClient != null) {
                pulsarClient.close();
            }
            if (pulsarServiceList != null && !pulsarServiceList.isEmpty()) {
                stopBroker();
            }
            if (mockBookKeeper != null) {
                mockBookKeeper.reallyShutdown();
            }
            if (mockZooKeeper != null) {
                mockZooKeeper.shutdown();
            }
            if (sameThreadOrderedSafeExecutor != null) {
                sameThreadOrderedSafeExecutor.shutdown();
            }
            if (bkExecutor != null) {
                bkExecutor.shutdown();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
            throw e;
        }
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;

    protected void restartBroker() throws Exception {
        stopBroker();
        startBroker();
    }

    protected void stopBroker() throws Exception {
        for (PulsarService pulsarService : pulsarServiceList) {
            pulsarService.close();
        }
        brokerPortList.clear();
        brokerWebservicePortList.clear();
        brokerWebServicePortTlsList.clear();
        aopProxyPortList.clear();
        pulsarServiceList.clear();
        amqpBrokerPortList.clear();
    }

    public void stopBroker(int brokerIndex) throws Exception {
        pulsarServiceList.get(brokerIndex).close();

        brokerPortList.remove(brokerIndex);
        brokerWebservicePortList.remove(brokerIndex);
        brokerWebServicePortTlsList.remove(brokerIndex);
        aopProxyPortList.remove(brokerIndex);
        pulsarServiceList.remove(brokerIndex);
    }

    protected void startBroker() throws Exception {
        for (int i = 0; i < brokerCount; i++) {

            int brokerPort = PortManager.nextFreePort();
            brokerPortList.add(brokerPort);

            int amqpBrokerPort = PortManager.nextFreePort();
            amqpBrokerPortList.add(amqpBrokerPort);

            int aopProxyPort = PortManager.nextFreePort();
            aopProxyPortList.add(aopProxyPort);

            int brokerWebServicePort = PortManager.nextFreePort();
            brokerWebservicePortList.add(brokerWebServicePort);

            int brokerWebServicePortTls = PortManager.nextFreePort();
            brokerWebServicePortTlsList.add(brokerWebServicePortTls);

            conf.setBrokerServicePort(Optional.of(brokerPort));
            ((AmqpServiceConfiguration) conf).setAmqpListeners("amqp://127.0.0.1:" + amqpBrokerPort);
            ((AmqpServiceConfiguration) conf).setAmqpProxyPort(aopProxyPort);
            ((AmqpServiceConfiguration) conf).setAmqpProxyEnable(true);
            conf.setWebServicePort(Optional.of(brokerWebServicePort));
            conf.setWebServicePortTls(Optional.of(brokerWebServicePortTls));

            log.info("Start broker info [{}], brokerPort: {}, amqpBrokerPort: {}, aopProxyPort: {}",
                    i, brokerPort, amqpBrokerPort, aopProxyPort);
            this.pulsarServiceList.add(startBroker(conf));
        }
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = spy(new PulsarService(conf));

        setupBrokerMocks(pulsar);
        pulsar.start();

        Compactor spiedCompactor = spy(pulsar.getCompactor());
        doReturn(spiedCompactor).when(pulsar).getCompactor();

        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockZooKeeperClientFactory).when(pulsar).getZooKeeperClientFactory();
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createLocalMetadataStore();
        doReturn(new ZKMetadataStore(mockZooKeeper)).when(pulsar).createConfigurationMetadataStore();

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
            "".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList, CreateMode.PERSISTENT);

        zk.create(
            "/ledgers/LAYOUT",
            "1\nflat:1".getBytes(ZookeeperClientFactoryImpl.ENCODING_SCHEME), dummyAclList,
            CreateMode.PERSISTENT);
        return zk;
    }

    public static NonClosableMockBookKeeper createMockBookKeeper(OrderedExecutor executor) throws Exception {
        return spy(new NonClosableMockBookKeeper(executor));
    }

    /**
     * Prevent the MockBookKeeper instance from being closed when the broker is restarted within a test.
     */
    public static class NonClosableMockBookKeeper extends PulsarMockBookKeeper {

        public NonClosableMockBookKeeper(OrderedExecutor executor) throws Exception {
            super(executor);
        }

        @Override
        public void close() {
            // no-op
        }

        @Override
        public void shutdown() {
            // no-op
        }

        public void reallyShutdown() {
            super.shutdown();
        }
    }

    protected ZooKeeperClientFactory mockZooKeeperClientFactory = new ZooKeeperClientFactory() {

        @Override
        public CompletableFuture<ZooKeeper> create(String serverList, SessionType sessionType,
                                                   int zkSessionTimeoutMillis) {
            // Always return the same instance
            // (so that we don't loose the mock ZK content on broker restart
            return CompletableFuture.completedFuture(mockZooKeeper);
        }
    };

    private BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup, Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass, Map<String, Object> ensemblePlacementPolicyProperties) throws IOException {
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration conf, ZooKeeper zkClient, EventLoopGroup eventLoopGroup, Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass, Map<String, Object> ensemblePlacementPolicyProperties, StatsLogger statsLogger) throws IOException {
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }

    };

    public static void retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
        throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null) || i == (retryCount - 1)) {
                break;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
    }

    public static void setFieldValue(Class clazz, Object classObj, String fieldName, Object fieldValue)
        throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

    public void checkPulsarServiceState() {
        for (PulsarService pulsarService : pulsarServiceList) {
            Mockito.when(pulsarService.getState()).thenReturn(PulsarService.State.Started);
        }
    }

    /**
     * Get available proxy port.
     * @return random proxy port
     */
    public int getProxyPort() {
        return getAopProxyPortList().get(RandomUtils.nextInt(0, getAopProxyPortList().size()));
    }

    /**
     * Set the starting broker count for test.
     */
    public void setBrokerCount(int brokerCount) {
        this.brokerCount = brokerCount;
    }

    public static String randExName() {
        return randomName("ex-", 4);
    }

    public static String randQuName() {
        return randomName("qu-", 4);
    }

    public static String randomName(String prefix, int numChars) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

}
