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
package io.streamnative.pulsar.handlers.amqp.integration;

import static java.util.stream.Collectors.joining;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.streamnative.pulsar.handlers.amqp.integration.container.BrokerContainer;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarCluster;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterSpec;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterTestBase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

@Slf4j
public abstract class PulsarClusterTestSuite extends PulsarClusterTestBase implements ITest {

    public PulsarClient client;
    public PulsarAdmin admin;

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(2)
            .clusterName(clusterName)
            .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());
        pulsarCluster = PulsarCluster.forSpec(spec);
        for(BrokerContainer brokerContainer : pulsarCluster.getBrokers()){
            getEnv().forEach(brokerContainer::withEnv);
        }

        pulsarCluster.start();

        this.client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();
        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();

        List<String> vhostList = Arrays.asList("vhost1", "vhost2", "vhost3");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns, 1);
            }
        }

        log.info("Cluster {} is setup", spec.clusterName());
    }

    protected abstract Map<String, String> getEnv();

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Override
    public String getTestName() {
        return "pulsar-cluster-test-suite";
    }

    public String randExName() {
        return randomName("ex-", 4);
    }

    public String randQuName() {
        return randomName("qu-", 4);
    }

    public String randomName(String prefix, int numChars) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }

    protected Connection getConnection(String vhost, boolean amqpProxyEnable) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        if (amqpProxyEnable) {
            int proxyPort = pulsarCluster.getAnyBroker().getMappedPort(5682);
            connectionFactory.setPort(proxyPort);
            log.info("use proxyPort: {}", proxyPort);
        } else {
            connectionFactory.setPort(pulsarCluster.getAnyBroker().getMappedPort(5672));
            log.info("use amqpBrokerPort: {}", connectionFactory.getPort());
        }
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

}
