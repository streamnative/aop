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

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.covertObjectValueAsString;

import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
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

    protected ExchangeContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
    }

    @Getter
    private Map<NamespaceName, Map<String, CompletableFuture<AmqpExchange>>> exchangeMap = new ConcurrentHashMap<>();


    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType) {
        return asyncGetExchange(namespaceName, exchangeName, createIfMissing, exchangeType, null);
    }

    /**
     * Get or create exchange.
     *
     * @param namespaceName namespace used in pulsar
     * @param exchangeName name of exchange
     * @param createIfMissing true to create the exchange if not existed, and exchangeType should be not null
     *                        false to get the exchange and return null if not existed
     * @param exchangeType type of exchange: direct,fanout,topic and headers
     * @param arguments other properties (construction arguments) for the exchange
     * @return the completableFuture of get result
     */
    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType,
                                                            Map<String, Object> arguments) {
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture = new CompletableFuture<>();
        if (StringUtils.isEmpty(exchangeType) && createIfMissing) {
            log.error("[{}][{}] ExchangeType should be set when createIfMissing is true.", namespaceName, exchangeName);
            amqpExchangeCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("exchangeType should be set when createIfMissing is true"));
            return amqpExchangeCompletableFuture;
        }
        if (namespaceName == null || StringUtils.isEmpty(exchangeName)) {
            log.error("[{}][{}] Parameter error, namespaceName or exchangeName is empty.", namespaceName, exchangeName);
            amqpExchangeCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("NamespaceName or exchangeName is empty"));
            return amqpExchangeCompletableFuture;
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            amqpExchangeCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
            return amqpExchangeCompletableFuture;
        }
        exchangeMap.putIfAbsent(namespaceName, new ConcurrentHashMap<>());
        CompletableFuture<AmqpExchange> existingAmqpExchangeFuture = exchangeMap.get(namespaceName).
                putIfAbsent(exchangeName, amqpExchangeCompletableFuture);
        if (existingAmqpExchangeFuture != null) {
            return existingAmqpExchangeFuture;
        } else {
            String topicName = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);

            Map<String, String> newProperties = null;
            if (createIfMissing && MapUtils.isNotEmpty(arguments)) {
                newProperties = new HashMap<>();
                String argumentsStr = covertObjectValueAsString(arguments);
                newProperties.put(PersistentExchange.CUSTOM_PROPERTIES, argumentsStr);
            }

            CompletableFuture<Topic> topicCompletableFuture = amqpTopicManager.getTopic(topicName, createIfMissing,
                    newProperties);
            topicCompletableFuture.whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    log.error("[{}][{}] Failed to get exchange topic.", namespaceName, exchangeName, throwable);
                    amqpExchangeCompletableFuture.completeExceptionally(throwable);
                    removeExchangeFuture(namespaceName, exchangeName);
                } else {
                    if (null == topic) {
                        log.warn("[{}][{}] The exchange topic did not exist.", namespaceName, exchangeName);
                        amqpExchangeCompletableFuture.complete(null);
                        removeExchangeFuture(namespaceName, exchangeName);
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
                        amqpExchangeCompletableFuture.complete(amqpExchange);
                    }
                }
            });
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
        removeExchangeFuture(namespaceName, exchangeName);
    }

    private void removeExchangeFuture(NamespaceName namespaceName, String exchangeName) {
        if (exchangeMap.containsKey(namespaceName)) {
            exchangeMap.get(namespaceName).remove(exchangeName);
        }
    }

}
