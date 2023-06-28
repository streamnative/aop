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
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SizeUnit;

/**
 * AMQP broker related.
 */
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
    private final PulsarClient pulsarClient;

    public AmqpBrokerService(PulsarService pulsarService, AmqpServiceConfiguration config) {
        try {
            this.pulsarClient = PulsarClient.builder()
                    .serviceUrl(config.getAmqpServerAddress())
                    .ioThreads(config.getNumIOThreads())
                    .listenerThreads(config.getNumIOThreads())
                    .memoryLimit(0, SizeUnit.BYTES)
                    .statsInterval(0, TimeUnit.MILLISECONDS)
                    .connectionMaxIdleSeconds(-1)
                    .build();
        } catch (PulsarClientException e) {
            throw new AoPServiceRuntimeException(e);
        }
        String clusterName = pulsarService.getBrokerService().getPulsar().getConfiguration().getClusterName();
        this.amqpAdmin = new AmqpAdmin(config.getAdvertisedAddress(), config.getAmqpAdminPort());
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService,
                initRouteExecutor(config), config, amqpAdmin, pulsarClient);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer, config);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer);
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer, amqpTopicManager);
        this.connectionContainer = new ConnectionContainer(pulsarService, exchangeContainer, queueContainer, config);
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
}
