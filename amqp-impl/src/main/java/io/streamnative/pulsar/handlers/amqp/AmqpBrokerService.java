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
import com.google.gson.Gson;
import java.util.Set;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationService;

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
    private AuthenticationService authenticationService;

    public AmqpBrokerService(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer);
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer);
        this.connectionContainer = new ConnectionContainer(pulsarService, exchangeContainer, queueContainer);
        loadAuthenticationService(amqpConfig);
    }

    public AmqpBrokerService(PulsarService pulsarService, ConnectionContainer connectionContainer,
                             AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsarService;
        this.amqpTopicManager = new AmqpTopicManager(pulsarService);
        this.exchangeContainer = new ExchangeContainer(amqpTopicManager, pulsarService);
        this.queueContainer = new QueueContainer(amqpTopicManager, pulsarService, exchangeContainer);
        this.exchangeService = new ExchangeServiceImpl(exchangeContainer);
        this.queueService = new QueueServiceImpl(exchangeContainer, queueContainer);
        this.connectionContainer = connectionContainer;
        loadAuthenticationService(amqpConfig);
    }

    @SneakyThrows
    public void loadAuthenticationService(AmqpServiceConfiguration amqpConfig) {
        Gson gson = new Gson();
        ServiceConfiguration conf = gson.fromJson(gson.toJson(pulsarService.getConfiguration(),
                ServiceConfiguration.class), ServiceConfiguration.class);
        Set<String> providers = Sets.newHashSet();
        providers.addAll(amqpConfig.getAuthenticationProviders());
        providers.addAll(amqpConfig.getAmqpAuthenticationProviders());
        conf.setAuthenticationProviders(providers);

        this.authenticationService = new AuthenticationService(conf);
    }

    public boolean isAuthenticationEnabled() {
        return pulsarService.getConfiguration().isAuthenticationEnabled();
    }
}
