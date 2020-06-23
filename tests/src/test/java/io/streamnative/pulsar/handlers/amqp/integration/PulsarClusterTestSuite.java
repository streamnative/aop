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

import io.streamnative.pulsar.handlers.amqp.integration.container.BrokerContainer;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarCluster;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterSpec;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterTestBase;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

@Slf4j
public abstract class PulsarClusterTestSuite extends PulsarClusterTestBase implements ITest {

    @BeforeSuite
    @Override
    public void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(1)
            .clusterName(clusterName)
            .build();

        log.info("Setting up cluster {} with {} bookies, {} brokers",
                spec.clusterName(), spec.numBookies(), spec.numBrokers());
        pulsarCluster = PulsarCluster.forSpec(spec);
        for(BrokerContainer brokerContainer : pulsarCluster.getBrokers()){
            getEnv().forEach(brokerContainer::withEnv);
        }

        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterSuite
    @Override
    public void tearDownCluster() {
        super.tearDownCluster();
    }

    @Override
    public String getTestName() {
        return "pulsar-cluster-test-suite";
    }

    protected abstract Map<String, String> getEnv();
}
