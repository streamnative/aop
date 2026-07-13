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

import io.netty.util.concurrent.DefaultThreadFactory;
import io.streamnative.pulsar.handlers.amqp.admin.AmqpAdmin;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.MetadataStoreCacheLoader;

/**
 * AMQP broker related.
 */
@Slf4j
public class AmqpBrokerService {
    @Getter
    private AmqpTopicManager amqpTopicManager;
    @Getter
    private ExchangeContainer exchangeContainer;
    @Getter
    private QueueContainer queueContainer;
    @Getter
    private ExchangeService exchangeService;
    @Getter
    private QueueService queueService;
    @Getter
    private ConnectionContainer connectionContainer;
    @Getter
    private PulsarService pulsarService;
    @Getter
    private AmqpAdmin amqpAdmin;
    @Getter
    private MetadataStoreCacheLoader metadataStoreCacheLoader;

    public AmqpBrokerService(PulsarService pulsarService, AmqpServiceConfiguration config) {
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService,
                initRouteExecutor(config), config);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer, config);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer);
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer);
        this.connectionContainer = new ConnectionContainer(pulsarService, exchangeContainer, queueContainer);
        this.amqpAdmin = new AmqpAdmin("localhost", config.getAmqpAdminPort());
        try {
            // Used by admin ownership redirects to resolve the owner broker's amqpAdminPort.
            this.metadataStoreCacheLoader = new MetadataStoreCacheLoader(pulsarService.getPulsarResources(),
                    30_000);
        } catch (Exception e) {
            // Unit tests may mock PulsarResources without load-report store; keep service usable.
            log.warn("Failed to init MetadataStoreCacheLoader for AoP, admin redirects may fail", e);
            this.metadataStoreCacheLoader = null;
        }
    }

    private ExecutorService initRouteExecutor(AmqpServiceConfiguration config) {
        return Executors.newFixedThreadPool(config.getAmqpExchangeRouteExecutorThreads(),
                new DefaultThreadFactory("exchange-route"));
    }

    public boolean isAuthenticationEnabled() {
        return pulsarService.getConfiguration().isAuthenticationEnabled();
    }

    public AuthenticationService getAuthenticationService() {
        return pulsarService.getBrokerService().getAuthenticationService();
    }

    public void close() {
        if (metadataStoreCacheLoader != null) {
            try {
                metadataStoreCacheLoader.close();
            } catch (IOException e) {
                log.warn("Failed to close MetadataStoreCacheLoader", e);
            }
        }
    }
}
