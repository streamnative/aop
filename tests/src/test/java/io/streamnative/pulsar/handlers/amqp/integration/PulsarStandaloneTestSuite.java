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

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.streamnative.pulsar.handlers.amqp.integration.container.PulsarContainer;
import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarStandaloneTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.testng.ITest;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

@Slf4j
public class PulsarStandaloneTestSuite extends PulsarStandaloneTestBase implements ITest {

    public PulsarClient client;
    public PulsarAdmin admin;

    @BeforeSuite
    public void setUpCluster() throws Exception {
        super.startCluster(PulsarContainer.DEFAULT_IMAGE_NAME);

        this.client = PulsarClient.builder()
                .serviceUrl(container.getPlainTextServiceUrl())
                .build();
        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl(container.getHttpServiceUrl())
                .build();

        List<String> vhostList = Arrays.asList("vhost1", "vhost2", "vhost3");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns, 1);
            }
        }
    }

    @AfterSuite
    public void tearDownCluster() throws Exception {
        super.stopCluster();
    }

    @Override
    public String getTestName() {
        return "pulsar-standalone-suite";
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
            int proxyPort = container.getMappedPort(5682);
            connectionFactory.setPort(proxyPort);
            log.info("use proxyPort: {}", proxyPort);
        } else {
            connectionFactory.setPort(container.getMappedPort(5672));
            log.info("use amqpBrokerPort: {}", connectionFactory.getPort());
        }
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

}
