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
package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import com.google.common.collect.Sets;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for RabbitMQ Client.
 */
@Slf4j
public class RabbitMQTestBase extends AmqpProtocolHandlerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();
        log.info("success internal setup");

        if (!admin.clusters().getClusters().contains(configClusterName)) {
            // so that clients can test short names
            admin.clusters().createCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + getBrokerWebservicePortList().get(0)));
        } else {
            admin.clusters().updateCluster(configClusterName,
                    new ClusterData("http://127.0.0.1:" + getBrokerWebServicePortTlsList().get(0)));
        }
        if (!admin.tenants().getTenants().contains("public")) {
            admin.tenants().createTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        } else {
            admin.tenants().updateTenant("public",
                    new TenantInfo(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        }

        List<String> vhostList = Arrays.asList("vhost1", "vhost2", "vhost3");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns);
                admin.namespaces().setRetention(ns,
                        new RetentionPolicies(60, 1000));
            }
        }
        checkPulsarServiceState();
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
//        super.internalCleanup();
    }

    protected Connection getConnection(String vhost, boolean useProxy) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        if (useProxy) {
            int proxyPort = getProxyPort();
//            int proxyPort = getProxyPortList().get(0);
            connectionFactory.setPort(proxyPort);
            log.info("use proxyPort: {}", proxyPort);
        } else {
            connectionFactory.setPort(getAmqpBrokerPortList().get(0));
            log.info("use amqpBrokerPort: {}", getAmqpBrokerPortList().get(0));
        }
        connectionFactory.setVirtualHost(vhost);
        return connectionFactory.newConnection();
    }

}
