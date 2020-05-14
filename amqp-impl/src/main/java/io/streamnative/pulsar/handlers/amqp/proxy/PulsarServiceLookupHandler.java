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
package io.streamnative.pulsar.handlers.amqp.proxy;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

/**
 * The proxy start with broker, use this lookup handler to find broker.
 */
@Slf4j
public class PulsarServiceLookupHandler implements LookupHandler {

    private PulsarService pulsarService;

    private PulsarClientImpl pulsarClient;

    private PulsarAdmin pulsarAdmin;

    public PulsarServiceLookupHandler(PulsarService pulsarService, PulsarClientImpl pulsarClient) {
        this.pulsarService = pulsarService;
        this.pulsarClient = pulsarClient;
    }

    @Override
    public Pair<String, Integer> findBroker(NamespaceName namespaceName, String protocolName) throws Exception {
        String hostname = null;
        Integer port = null;

        try {
            int httpPort = 0;
            NamespaceBundles bundles = pulsarService.getNamespaceService()
                    .getNamespaceBundleFactory().getBundles(namespaceName);
            NamespaceBundle bundle = bundles.getFullBundle();

            Optional<URL> url =  pulsarService.getNamespaceService()
                    .getWebServiceUrl(bundle, true, false, false);
            if (url.isPresent()) {
                hostname = url.get().getHost();
                httpPort = url.get().getPort();
            }

            String zkPath = LoadManager.LOADBALANCE_BROKERS_ROOT + "/" + hostname + ":" + httpPort;
            log.info("findBroker zkPath: {}", zkPath);
            Optional<? extends ServiceLookupData> serviceLookupData = pulsarService.getLocalZkCache().getData(
                    zkPath, pulsarService.getLoadManager().get().getLoadReportDeserializer());
            if (serviceLookupData.isPresent()) {
                Optional<String> protocolAdvertise = serviceLookupData.get().getProtocol(protocolName);
                if (protocolAdvertise.isPresent()) {
                    String advertise = protocolAdvertise.get();
                    String[] splits = advertise.split(":");
                    port = Integer.parseInt(splits[splits.length - 1]);
                }
            } else {
                throw new ProxyException("Failed to find broker for namespaceName: " + namespaceName.toString());
            }
            log.info("Find broker namespaceName: {}, bundle: {}, hostname: {}, port: {}", namespaceName, bundle, hostname, port);
            return Pair.of(hostname, port);
        } catch (Exception e) {
            String errorMsg = String.format("Failed to find broker for namespaceName: %S. msg: %S",
                    namespaceName.toString(), e.getMessage());
            log.error(errorMsg, e);
            throw new ProxyException(errorMsg);
        }
    }

    @Override
    public Pair<String, Integer> findBroker(TopicName topicName, String protocolHandlerName) throws Exception {
        Pair<InetSocketAddress, InetSocketAddress> lookup = pulsarClient.getLookup().getBroker(topicName).get();
        String hostName = lookup.getLeft().getHostName();
        List<String> children = pulsarService.getZkClient().getChildren(LoadManager.LOADBALANCE_BROKERS_ROOT, null);
        int amqpBrokerPort = 0;
        for (String webService : children) {
            try {
                byte[] content = pulsarService.getZkClient().getData(LoadManager.LOADBALANCE_BROKERS_ROOT
                        + "/" + webService, null, null);
                ServiceLookupData serviceLookupData = pulsarService.getLoadManager().get()
                        .getLoadReportDeserializer().deserialize("", content);
                if (serviceLookupData.getPulsarServiceUrl().contains("" + lookup.getLeft().getPort())) {
                    if (serviceLookupData.getProtocol(protocolHandlerName).isPresent()) {
                        String amqpBrokerUrl = serviceLookupData.getProtocol(protocolHandlerName).get();
                        String[] splits = amqpBrokerUrl.split(":");
                        String port = splits[splits.length - 1];
                        amqpBrokerPort = Integer.parseInt(port);
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return Pair.of(hostName, amqpBrokerPort);
    }
}
