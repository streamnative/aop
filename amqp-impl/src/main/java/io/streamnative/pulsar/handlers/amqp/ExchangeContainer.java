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

import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    private Executor executor = Executors.newCachedThreadPool();
    private AmqpTopicManager amqpTopicManager;
    private PulsarService pulsarService;

    protected ExchangeContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
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

    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType) {
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture = new CompletableFuture<>();
        if (StringUtils.isEmpty(exchangeType) && createIfMissing) {
            log.error("exchangeType should be set when createIfMissing is true");
            amqpExchangeCompletableFuture.complete(null);
        }
        if (namespaceName == null || StringUtils.isEmpty(exchangeName)) {
            log.error("Parameter error, namespaceName or exchangeName is empty.");
            amqpExchangeCompletableFuture.complete(null);
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            amqpExchangeCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
        }
        Map<String, AmqpExchange> map = exchangeMap.getOrDefault(namespaceName, null);
        if (map == null || map.getOrDefault(exchangeName, null) == null) {
            // check pulsar topic
            String topicName = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);
            executor.execute(() -> {
                CompletableFuture<Topic> topicCompletableFuture =
                        amqpTopicManager.getTopic(topicName, createIfMissing);
                topicCompletableFuture.whenComplete((topic, throwable) -> {
                    if (throwable != null) {
                        log.error("Get topic error:{}", throwable.getMessage());
                        amqpExchangeCompletableFuture.complete(null);
                    } else {
                        if (null == topic) {
                            log.error("The exchange topic did not exist. namespace{}, exchangeName: {}",
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
            });
        } else {
            amqpExchangeCompletableFuture.complete(map.getOrDefault(exchangeName, null));
        }

        return amqpExchangeCompletableFuture;
    }

    public void deleteExchange(NamespaceName namespaceName, String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        if (exchangeMap.containsKey(namespaceName)) {
            exchangeMap.get(namespaceName).remove(exchangeName);
        }
    }

}
