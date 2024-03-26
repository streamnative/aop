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
package io.streamnative.pulsar.handlers.amqp.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.channel.EventLoopGroup;
import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.BookKeeperClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.SameThreadOrderedSafeExecutor;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.MockZooKeeper;
import org.apache.zookeeper.MockZooKeeperSession;
import org.apache.zookeeper.data.ACL;

/**
 * Unit test to test Aop handler.
 */
@Slf4j
public abstract class AopProtocolHandlerTestBase {

    protected ServiceConfiguration conf;
    protected PulsarService pulsar;
    @Getter
    protected int brokerPort = 8888;

    protected MockZooKeeper mockZooKeeper;
    protected NonClosableMockBookKeeper mockBookKeeper;

    private SameThreadOrderedSafeExecutor sameThreadOrderedSafeExecutor;
    private OrderedExecutor bkExecutor;

    public AopProtocolHandlerTestBase() {
        resetConfig();
    }

    protected void resetConfig() {
        AmqpServiceConfiguration amqpServiceConfiguration = new AmqpServiceConfiguration();
        amqpServiceConfiguration.setProtocolHandlerDirectory(
            "target/"
        );
        amqpServiceConfiguration.setMessagingProtocols(Sets.newHashSet("amqp"));
        amqpServiceConfiguration.setBrokerShutdownTimeoutMs(0L);
        this.conf = amqpServiceConfiguration;
        this.conf.setClusterName("test-cluster");
    }

    protected final void internalSetup() throws Exception {
        init();
    }


    protected final void init() throws Exception {
        sameThreadOrderedSafeExecutor = new SameThreadOrderedSafeExecutor();
        bkExecutor = OrderedExecutor.newBuilder().numThreads(1).name("mock-pulsar-bk").build();

        mockZooKeeper = createMockZooKeeper();
        mockBookKeeper = createMockBookKeeper(bkExecutor);

        startBroker();

    }

    protected final void internalCleanup() throws Exception {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (pulsar != null) {
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
        pulsar.close();
    }

    protected void startBroker() throws Exception {
        this.pulsar = startBroker(conf);
    }

    protected PulsarService startBroker(ServiceConfiguration conf) throws Exception {
        PulsarService pulsar = spy(new PulsarService(conf));

        setupBrokerMocks(pulsar);
        pulsar.start();

        return pulsar;
    }

    protected void setupBrokerMocks(PulsarService pulsar) throws Exception {
        // Override default providers with mocked ones
        doReturn(mockBookKeeperClientFactory).when(pulsar).newBookKeeperClientFactory();

        MockZooKeeperSession mockZooKeeperSession = MockZooKeeperSession.newInstance(mockZooKeeper);
        doReturn(new ZKMetadataStore(mockZooKeeperSession)).when(pulsar).createLocalMetadataStore(any());
        doReturn(new ZKMetadataStore(mockZooKeeperSession)).when(pulsar).createConfigurationMetadataStore(any());

        Supplier<NamespaceService> namespaceServiceSupplier = () -> spy(new NamespaceService(pulsar));
        doReturn(namespaceServiceSupplier).when(pulsar).getNamespaceServiceProvider();

        doReturn(sameThreadOrderedSafeExecutor).when(pulsar).getOrderedExecutor();
    }

    public static MockZooKeeper createMockZooKeeper() throws Exception {
        MockZooKeeper zk = MockZooKeeper.newInstance(MoreExecutors.newDirectExecutorService());
        List<ACL> dummyAclList = new ArrayList<>(0);

        ZkUtils.createFullPathOptimistic(zk, "/ledgers/available/192.168.1.1:" + 5000,
                "".getBytes(StandardCharsets.UTF_8), dummyAclList, CreateMode.PERSISTENT);

        zk.create("/ledgers/LAYOUT", "1\nflat:1".getBytes(StandardCharsets.UTF_8), dummyAclList,
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

    private final BookKeeperClientFactory mockBookKeeperClientFactory = new BookKeeperClientFactory() {

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public BookKeeper create(ServiceConfiguration conf, MetadataStoreExtended store,
                                 EventLoopGroup eventLoopGroup,
                                 Optional<Class<? extends EnsemblePlacementPolicy>> ensemblePlacementPolicyClass,
                                 Map<String, Object> properties, StatsLogger statsLogger) {
            // Always return the same instance (so that we don't loose the mock BK content on broker restart
            return mockBookKeeper;
        }

        @Override
        public void close() {
            // no-op
        }
    };


    public static void setFieldValue(Class clazz, Object classObj, String fieldName, Object fieldValue)
        throws Exception {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(classObj, fieldValue);
    }

}
