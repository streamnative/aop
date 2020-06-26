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

import com.google.common.collect.Sets;
import com.google.gson.JsonObject;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * Base test class for RabbitMQ Client.
 */
@Slf4j
public class AmqpTestBase extends AmqpProtocolHandlerTestBase {

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
                admin.namespaces().createNamespace(ns, 1);
                admin.lookups().lookupTopicAsync(TopicName.get(TopicDomain.persistent.value(),
                        NamespaceName.get(ns), "__lookup__").toString());
                checkDefaultExchange(vhost);
            }
        }

        checkPulsarServiceState();
    }

    private void checkDefaultExchange(String vhost) {
        Map<String, AmqpExchange.Type> map = ExchangeContainer.getBuildinExchangeNameMap();
        for (String exchangeName : map.keySet()) {
            String topicName = "persistent://public/" + vhost + "/" + PersistentExchange.TOPIC_PREFIX + exchangeName;

            JsonObject jsonObject = null;
            for (int i = 0; i < 30; i++) {
                try {
                    jsonObject = admin.topics().getInternalInfo(topicName);
                    log.info("topic [{}] internalInfo: {}", topicName, jsonObject.toString());
                    Thread.sleep(1000L);
                    break;
                } catch (Exception e) {
                    log.info("Failed to get topic internalInfo. topicName: {}, errMsg: {}", topicName, e.getMessage());
                }
            }
            if (jsonObject == null) {
                Assert.fail("Default exchange init failed. topicName: " + topicName);
            }
        }
    }

    @AfterClass
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

}
