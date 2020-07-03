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

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Container for all exchanges in the broker.
 */
@Slf4j
public class ExchangeContainer {

    private AmqpTopicManager amqpTopicManager;
    private PulsarService pulsarService;
    private OrderedExecutor orderedExecutor;

    protected ExchangeContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
        this.orderedExecutor = OrderedExecutor.newBuilder()
                // TODO make it configurable
                .numThreads(100)
                .name("queue-container-workers")
                .build();
    }

    @Getter
    private Map<NamespaceName, Map<String, AmqpExchange>> exchangeMap = new ConcurrentHashMap<>();

    private void putExchange(NamespaceName namespaceName, String exchangeName, AmqpExchange amqpExchange) {
        exchangeMap.compute(namespaceName, (name, map) -> {
            Map<String, AmqpExchange> amqpExchangeMap = map;
            if (amqpExchangeMap == null) {
                amqpExchangeMap = Maps.newConcurrentMap();
            }
            amqpExchangeMap.put(exchangeName, amqpExchange);
            return amqpExchangeMap;
        });
    }

    /**
     * Get or create exchange.
     *
     * @param namespaceName namespace used in pulsar
     * @param exchangeName name of exchange
     * @param createIfMissing true to create the exchange if not existed, and exchangeType should be not null
     *                        false to get the exchange and return null if not existed
     * @param exchangeType type of exchange: direct,fanout,topic and headers
     * @return the completableFuture of get result
     */
    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType) {
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture = new CompletableFuture<>();
        if (StringUtils.isEmpty(exchangeType) && createIfMissing) {
            log.error("exchangeType should be set when createIfMissing is true");
            amqpExchangeCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("exchangeType should be set when createIfMissing is true"));
            return amqpExchangeCompletableFuture;
        }
        if (namespaceName == null || StringUtils.isEmpty(exchangeName)) {
            log.error("Parameter error, namespaceName or exchangeName is empty.");
            amqpExchangeCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("NamespaceName or exchangeName is empty"));
            return amqpExchangeCompletableFuture;
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            amqpExchangeCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
            return amqpExchangeCompletableFuture;
        }
        Map<String, AmqpExchange> map = exchangeMap.getOrDefault(namespaceName, null);
        if (map == null || map.getOrDefault(exchangeName, null) == null) {
            String topicName = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);
            orderedExecutor.executeOrdered(topicName, safeRun(() -> {
                Map<String, AmqpExchange> innerMap = exchangeMap.getOrDefault(namespaceName, null);
                if (innerMap == null || innerMap.getOrDefault(exchangeName, null) == null) {
                    CompletableFuture<Topic> topicCompletableFuture =
                            amqpTopicManager.getTopic(topicName, createIfMissing);
                    topicCompletableFuture.whenComplete((topic, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to get topic from amqpTopicManager", throwable);
                            amqpExchangeCompletableFuture.completeExceptionally(throwable);
                        } else {
                            if (null == topic) {
                                log.info("The exchange topic did not exist. namespace{}, exchangeName: {}",
                                        namespaceName.toString(), exchangeName);
                                amqpExchangeCompletableFuture.complete(null);
                            } else {
                                // recover metadata if existed
                                PersistentTopic persistentTopic = (PersistentTopic) topic;
                                Map<String, String> properties = persistentTopic.getManagedLedger().getProperties();
                                AmqpExchange.Type amqpExchangeType;
                                // if properties has type, ignore the exchangeType
                                if (null != properties && properties.size() > 0
                                        && null != properties.get(PersistentExchange.TYPE)) {
                                    String type = properties.get(PersistentExchange.TYPE);
                                    amqpExchangeType = AmqpExchange.Type.value(type);
                                } else {
                                    amqpExchangeType = AmqpExchange.Type.value(exchangeType);
                                }
                                PersistentExchange amqpExchange = new PersistentExchange(exchangeName,
                                        amqpExchangeType, persistentTopic, false);
                                putExchange(namespaceName, exchangeName, amqpExchange);
                                amqpExchangeCompletableFuture.complete(amqpExchange);
                            }
                        }
                    });
                } else {
                    amqpExchangeCompletableFuture.complete(innerMap.getOrDefault(exchangeName, null));
                }
            }));
        } else {
            amqpExchangeCompletableFuture.complete(map.getOrDefault(exchangeName, null));
        }

        return amqpExchangeCompletableFuture;
    }

    /**
     * Delete the exchange by namespace and exchange name.
     *
     * @param namespaceName namespace name in pulsar
     * @param exchangeName name of exchange
     */
    public void deleteExchange(NamespaceName namespaceName, String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        if (exchangeMap.containsKey(namespaceName)) {
            exchangeMap.get(namespaceName).remove(exchangeName);
        }
    }

}
