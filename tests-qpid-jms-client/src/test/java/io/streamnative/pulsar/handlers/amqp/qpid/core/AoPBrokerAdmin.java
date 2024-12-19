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
package io.streamnative.pulsar.handlers.amqp.qpid.core;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.BrokerAdmin.PortType.ANONYMOUS_AMQP;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandlerTestBase;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;

/**
 * AoP BrokerAdmin.
 */
@Slf4j
public class AoPBrokerAdmin extends AmqpProtocolHandlerTestBase implements BrokerAdmin {

    @Override
    protected void setup() throws Exception {
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

        List<String> vhostList = List.of("vhost1");
        for (String vhost : vhostList) {
            String ns = "public/" + vhost;
            if (!admin.namespaces().getNamespaces("public").contains(ns)) {
                admin.namespaces().createNamespace(ns, 1);
                admin.lookups().lookupTopicAsync(TopicName.get(TopicDomain.persistent.value(),
                        NamespaceName.get(ns), "__lookup__").toString());
            }
        }
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    public PortType getPreferredPortType() {
        return ANONYMOUS_AMQP;
    }

    @Override
    public void beforeTestClass(Class testClass) {
        log.info("beforeTestClass class:{}", testClass.getName());
        try {
            setup();
        } catch (Exception e) {
            log.error("[beforeTestClass] setup error", e);
            throw new QpidJMSTestException(e);
        }
    }

    @Override
    public void beforeTestMethod(Class testClass, Method method) {
        log.info("beforeTestMethod class:{}, method: {}", testClass.getName(), method.getName());
    }

    @Override
    public void afterTestMethod(Class testClass, Method method) {
        log.info("afterTestMethod class:{}, method: {}", testClass.getName(), method.getName());
    }

    @Override
    public void afterTestClass(Class testClass) {
        log.info("afterTestClass class:{}", testClass.getName());
        try {
            this.cleanup();
        } catch (Exception e) {
            log.error("[afterTestClass] cleanup error.", e);
            throw new QpidJMSTestException(e);
        }
    }

    @Override
    public InetSocketAddress getBrokerAddress(PortType portType) {
        return new InetSocketAddress( "127.0.0.1", getAmqpBrokerPortList().get(0));
    }

    @Override
    public void createQueue(String queueName) {

    }

    @Override
    public void deleteQueue(String queueName) {

    }

    @Override
    public void putMessageOnQueue(String queueName, String... messages) {

    }

    @Override
    public int getQueueDepthMessages(String testQueueName) {
        return 0;
    }

    @Override
    public boolean supportsRestart() {
        return true;
    }

    @Override
    public ListenableFuture<Void> restart() {
        try {
            log.info("Start restart AoP brokers.");
            restartBroker();
            log.info("Finish restart AoP brokers.");
        } catch (Exception e) {
            log.error("Failed to restart broker.", e);
            return Futures.immediateFailedFuture(e);
        }
        return Futures.immediateFuture(null);
    }

    @Override
    public boolean isAnonymousSupported() {
        return false;
    }

    @Override
    public boolean isSASLSupported() {
        return false;
    }

    @Override
    public boolean isSASLMechanismSupported(String mechanismName) {
        return false;
    }

    @Override
    public boolean isWebSocketSupported() {
        return false;
    }

    @Override
    public boolean isQueueDepthSupported() {
        return false;
    }

    @Override
    public boolean isManagementSupported() {
        return false;
    }

    @Override
    public boolean isPutMessageOnQueueSupported() {
        return false;
    }

    @Override
    public boolean isDeleteQueueSupported() {
        return false;
    }

    @Override
    public String getValidUsername() {
        return null;
    }

    @Override
    public String getValidPassword() {
        return null;
    }

    @Override
    public String getKind() {
        return null;
    }
}
