/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.pulsar.handlers.amqp.integration.topologies;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.integration.container.BKContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.BrokerContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.CSContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.ContainerExecResult;
import io.streamnative.pulsar.handlers.amqp.integration.container.ProxyContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.PulsarContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.WorkerContainer;
import io.streamnative.pulsar.handlers.amqp.integration.container.ZKContainer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.streamnative.pulsar.handlers.amqp.integration.container.PulsarContainer.BROKER_HTTP_PORT;
import static io.streamnative.pulsar.handlers.amqp.integration.container.PulsarContainer.CS_PORT;
import static io.streamnative.pulsar.handlers.amqp.integration.container.PulsarContainer.ZK_PORT;

/**
 * Pulsar Cluster in containers.
 */
@Slf4j
public class PulsarCluster {

    public static final String ADMIN_SCRIPT = "/pulsar/bin/pulsar-admin";
    public static final String CLIENT_SCRIPT = "/pulsar/bin/pulsar-client";
    public static final String PULSAR_COMMAND_SCRIPT = "/pulsar/bin/pulsar";

    /**
     * Pulsar Cluster Spec
     *
     * @param spec pulsar cluster spec.
     * @return the built pulsar cluster
     */
    public static PulsarCluster forSpec(PulsarClusterSpec spec) {
        return new PulsarCluster(spec);
    }

    private final PulsarClusterSpec spec;

    @Getter
    private final String clusterName;
    private final Network network;
    private final ZKContainer zkContainer;
    private final CSContainer csContainer;
    private final Map<String, BKContainer> bookieContainers;
    private final Map<String, BrokerContainer> brokerContainers;
    private final ProxyContainer proxyContainer;
    private Map<String, GenericContainer<?>> externalServices = Collections.emptyMap();

    private PulsarCluster(PulsarClusterSpec spec) {

        this.spec = spec;
        this.clusterName = spec.clusterName();
        this.network = Network.newNetwork();

        this.zkContainer = new ZKContainer(clusterName);
        this.zkContainer
            .withNetwork(network)
            .withNetworkAliases(ZKContainer.NAME)
            .withEnv("clusterName", clusterName)
            .withEnv("zkServers", ZKContainer.NAME)
            .withEnv("configurationStore", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("forceSync", "no")
            .withEnv("pulsarNode", "pulsar-broker-0");

        this.csContainer = new CSContainer(clusterName)
            .withNetwork(network)
            .withNetworkAliases(CSContainer.NAME);

        this.bookieContainers = Maps.newTreeMap();
        this.brokerContainers = Maps.newTreeMap();

        this.proxyContainer = new ProxyContainer(clusterName, ProxyContainer.NAME)
            .withNetwork(network)
            .withNetworkAliases("pulsar-proxy")
            .withEnv("zkServers", ZKContainer.NAME)
            .withEnv("zookeeperServers", ZKContainer.NAME)
            .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
            .withEnv("clusterName", clusterName);

        // create bookies
        bookieContainers.putAll(
                runNumContainers("bookie", spec.numBookies(), (name) -> new BKContainer(clusterName, name)
                        .withNetwork(network)
                        .withNetworkAliases(name)
                        .withEnv("zkServers", ZKContainer.NAME)
                        .withEnv("useHostNameAsBookieID", "true")
                        // Disable fsyncs for tests since they're slow within the containers
                        .withEnv("journalSyncData", "false")
                        .withEnv("journalMaxGroupWaitMSec", "0")
                        .withEnv("clusterName", clusterName)
                        .withEnv("diskUsageThreshold", "0.99")
                )
        );

        // create brokers
        brokerContainers.putAll(
                runNumContainers("broker", spec.numBrokers(), (name) -> new BrokerContainer(clusterName, name)
                        .withNetwork(network)
                        .withNetworkAliases(name)
                        .withEnv("zkServers", ZKContainer.NAME)
                        .withEnv("zookeeperServers", ZKContainer.NAME)
                        .withEnv("configurationStoreServers", CSContainer.NAME + ":" + CS_PORT)
                        .withEnv("clusterName", clusterName)
                        .withEnv("brokerServiceCompactionMonitorIntervalInSeconds", "1")
                )
        );

        spec.classPathVolumeMounts.forEach((key, value) -> {
            zkContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);
            proxyContainer.withClasspathResourceMapping(key, value, BindMode.READ_WRITE);

            bookieContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
            brokerContainers.values().forEach(c -> c.withClasspathResourceMapping(key, value, BindMode.READ_WRITE));
        });

    }

    public String getPlainTextServiceUrl() {
        return proxyContainer.getPlainTextServiceUrl();
    }

    public String getHttpServiceUrl() {
        return proxyContainer.getHttpServiceUrl();
    }

    public String getAllBrokersHttpServiceUrl() {
        String multiUrl = "http://";
        Iterator<BrokerContainer> brokers = getBrokers().iterator();
        while (brokers.hasNext()) {
            BrokerContainer broker = brokers.next();
            multiUrl += broker.getContainerIpAddress() + ":" + broker.getMappedPort(BROKER_HTTP_PORT);
            if (brokers.hasNext()) {
                multiUrl += ",";
            }
        }
        return multiUrl;
    }

    public String getZKConnString() {
        return zkContainer.getContainerIpAddress() + ":" + zkContainer.getMappedPort(ZK_PORT);
    }

    public Network getNetwork() {
        return network;
    }

    public Map<String, GenericContainer<?>> getExternalServices() {
        return externalServices;
    }

    public void start() throws Exception {
        // start the local zookeeper
        zkContainer.start();
        log.info("Successfully started local zookeeper container.");

        // start the configuration store
        csContainer.start();
        log.info("Successfully started configuration store container.");

        // init the cluster
        zkContainer.execCmd(
            "bin/init-cluster.sh");
        log.info("Successfully initialized the cluster.");

        // start bookies
        bookieContainers.values().forEach(BKContainer::start);
        log.info("Successfully started {} bookie containers.", bookieContainers.size());

        // start brokers
        this.startAllBrokers();
        log.info("Successfully started {} broker containers.", brokerContainers.size());

        // create proxy
        proxyContainer.start();
        log.info("Successfully started pulsar proxy.");

        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", getHttpServiceUrl());

        // start external services
        this.externalServices = spec.externalServices;
        if (null != externalServices) {
            externalServices.entrySet().parallelStream().forEach(service -> {
                GenericContainer<?> serviceContainer = service.getValue();
                serviceContainer.withNetwork(network);
                serviceContainer.withNetworkAliases(service.getKey());
                serviceContainer.start();
                log.info("Successfully start external service {}.", service.getKey());
            });
        }
    }

    public void startService(String networkAlias,
                             GenericContainer<?> serviceContainer) {
        log.info("Starting external service {} ...", networkAlias);
        serviceContainer.withNetwork(network);
        serviceContainer.withNetworkAliases(networkAlias);
        serviceContainer.start();
        log.info("Successfully start external service {}", networkAlias);
    }

    public void stopService(String networkAlias,
                            GenericContainer<?> serviceContainer) {
        log.info("Stopping external service {} ...", networkAlias);
        serviceContainer.stop();
        log.info("Successfully stop external service {}", networkAlias);
    }


    private static <T extends PulsarContainer> Map<String, T> runNumContainers(String serviceName,
                                                                               int numContainers,
                                                                               Function<String, T> containerCreator) {
        Map<String, T> containers = Maps.newTreeMap();
        for (int i = 0; i < numContainers; i++) {
            String name = "pulsar-" + serviceName + "-" + i;
            T container = containerCreator.apply(name);
            containers.put(name, container);
        }
        return containers;
    }

    public synchronized void stop() {

        List<GenericContainer> containers = new ArrayList<>();

        containers.addAll(brokerContainers.values());
        containers.addAll(bookieContainers.values());

        if (externalServices != null) {
            containers.addAll(externalServices.values());
        }

        if (null != proxyContainer) {
            containers.add(proxyContainer);
        }
        if (null != csContainer) {
            containers.add(csContainer);
        }
        if (null != zkContainer) {
            containers.add(zkContainer);
        }

        containers = containers.parallelStream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        containers.parallelStream().forEach(GenericContainer::stop);

        try {
            network.close();
        } catch (Exception e) {
            log.info("Failed to shutdown network for pulsar cluster {}", clusterName, e);
        }
    }

    public void startContainers(Map<String, GenericContainer<?>> containers) {
        containers.forEach((name, container) -> {
            container
                .withNetwork(network)
                .withNetworkAliases(name)
                .start();
            log.info("Successfully start container {}.", name);
        });
    }

    public void stopContainers(Map<String, GenericContainer<?>> containers) {
        containers.values().parallelStream().forEach(GenericContainer::stop);
        log.info("Successfully stop containers : {}", containers);
    }

    public BrokerContainer getAnyBroker() {
        return getAnyContainer(brokerContainers, "pulsar-broker");
    }

    public BrokerContainer getBroker(int index) {
        return getAnyContainer(brokerContainers, "pulsar-broker", index);
    }

    private <T> T getAnyContainer(Map<String, T> containers, String serviceName) {
        List<T> containerList = Lists.newArrayList();
        containerList.addAll(containers.values());
        Collections.shuffle(containerList);
        checkArgument(!containerList.isEmpty(), "No " + serviceName + " is alive");
        return containerList.get(0);
    }

    private <T> T getAnyContainer(Map<String, T> containers, String serviceName, int index) {
        checkArgument(!containers.isEmpty(), "No " + serviceName + " is alive");
        checkArgument((index >= 0 && index < containers.size()), "Index : " + index + " is out range");
        return containers.get(serviceName.toLowerCase() + "-" + index);
    }

    public Collection<BrokerContainer> getBrokers() {
        return brokerContainers.values();
    }

    public ProxyContainer getProxy() {
        return proxyContainer;
    }

    public Collection<BKContainer> getBookies() {
        return bookieContainers.values();
    }

    public ZKContainer getZooKeeper() {
        return zkContainer;
    }

    public ContainerExecResult runAdminCommandOnAnyBroker(String...commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(ADMIN_SCRIPT, commands);
    }

    public ContainerExecResult runPulsarBaseCommandOnAnyBroker(String...commands) throws Exception {
        return runCommandOnAnyBrokerWithScript(PULSAR_COMMAND_SCRIPT, commands);
    }

    private ContainerExecResult runCommandOnAnyBrokerWithScript(String scriptType, String...commands) throws Exception {
        BrokerContainer container = getAnyBroker();
        String[] cmds = new String[commands.length + 1];
        cmds[0] = scriptType;
        System.arraycopy(commands, 0, cmds, 1, commands.length);
        return container.execCmd(cmds);
    }

    public void stopAllBrokers() {
        brokerContainers.values().forEach(BrokerContainer::stop);
    }

    public void startAllBrokers() {
        brokerContainers.values().forEach(BrokerContainer::start);
    }

    public void stopAllBookies() {
        bookieContainers.values().forEach(BKContainer::stop);
    }

    public void startAllBookies() {
        bookieContainers.values().forEach(BKContainer::start);
    }

    public void stopZooKeeper() {
        zkContainer.stop();
    }

    public void startZooKeeper() {
        zkContainer.start();
    }

    public ContainerExecResult createNamespace(String nsName) throws Exception {
        return runAdminCommandOnAnyBroker(
            "namespaces", "create", "public/" + nsName,
            "--clusters", clusterName);
    }

    public ContainerExecResult createPartitionedTopic(String topicName, int partitions) throws Exception {
        return runAdminCommandOnAnyBroker(
                "topics", "create-partitioned-topic", topicName,
                "-p", String.valueOf(partitions));
    }

    public ContainerExecResult enableDeduplication(String nsName, boolean enabled) throws Exception {
        return runAdminCommandOnAnyBroker(
            "namespaces", "set-deduplication", "public/" + nsName,
            enabled ? "--enable" : "--disable");
    }

}
