/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streamnative.pulsar.handlers.amqp;

import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.ARGUMENTS;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.AUTO_DELETE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.DURABLE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.INTERNAL;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.TYPE;

import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.protocol.ErrorCodes;

/**
 * Container for all exchanges in the broker.
 */
@Slf4j
public class ExchangeContainer {

    private AmqpTopicManager amqpTopicManager;
    private PulsarService pulsarService;
    private final ExecutorService routeExecutor;
    private final AmqpServiceConfiguration config;

    protected ExchangeContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService,
                                ExecutorService routeExecutor, AmqpServiceConfiguration config) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
        this.routeExecutor = routeExecutor;
        this.config = config;
    }

    @Getter
    private Map<NamespaceName, Map<String, CompletableFuture<AmqpExchange>>> exchangeMap = new ConcurrentHashMap<>();


    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType) {
        return asyncGetExchange(namespaceName, exchangeName, createIfMissing, exchangeType, true, false, false, null);
    }

    /**
     * Get or create exchange.
     *
     * @param namespaceName   namespace used in pulsar
     * @param exchangeName    name of exchange
     * @param createIfMissing true to create the exchange if not existed, and exchangeType should be not null
     *                        false to get the exchange and return null if not existed
     * @param exchangeType    type of exchange: direct,fanout,topic and headers
     * @param arguments       other properties (construction arguments) for the exchange
     * @return the completableFuture of get result
     */
    public CompletableFuture<AmqpExchange> asyncGetExchange(NamespaceName namespaceName,
                                                            String exchangeName,
                                                            boolean createIfMissing,
                                                            String exchangeType,
                                                            boolean durable,
                                                            boolean autoDelete,
                                                            boolean internal,
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
            if (createIfMissing) {
                return existingAmqpExchangeFuture.thenCompose(amqpExchange -> {
                    if (amqpExchange instanceof PersistentExchange persistentExchange) {
                        if (!exchangeDeclareCheck(
                                amqpExchangeCompletableFuture, namespaceName.getLocalName(),
                                exchangeName, exchangeType, durable, autoDelete, persistentExchange.getProperties())) {
                            return amqpExchangeCompletableFuture;
                        }
                    }
                    return existingAmqpExchangeFuture;
                });
            }
            return existingAmqpExchangeFuture;
        } else {
            String topicName = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);
            Map<String, String> initProperties = new HashMap<>();
            if (createIfMissing) {
                // if first create the exchange, try to set properties for exchange
                try {
                    initProperties = ExchangeUtil.generateTopicProperties(exchangeName, exchangeType, durable,
                            autoDelete, internal, arguments, Collections.EMPTY_LIST);
                } catch (Exception e) {
                    log.error("Failed to generate topic properties for exchange {} in vhost {}.",
                            exchangeName, namespaceName, e);
                    amqpExchangeCompletableFuture.completeExceptionally(e);
                    removeExchangeFuture(namespaceName, exchangeName);
                    return amqpExchangeCompletableFuture;
                }
            }

            CompletableFuture<Topic> topicCompletableFuture = amqpTopicManager.getTopic(
                    topicName, createIfMissing, initProperties);
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
                        if (createIfMissing && !exchangeDeclareCheck(
                                amqpExchangeCompletableFuture, namespaceName.getLocalName(),
                                exchangeName, exchangeType, durable, autoDelete, properties)) {
                            removeExchangeFuture(namespaceName, exchangeName);
                            return;
                        }

                        PersistentExchange amqpExchange;
                        try {
                            Map<String, Object> currentArguments =
                                    ExchangeUtil.covertStringValueAsObjectMap(properties.get(ARGUMENTS));
                            String currentType = properties.get(TYPE);
                            boolean currentDurable = Boolean.parseBoolean(
                                    properties.getOrDefault(DURABLE, "true"));
                            boolean currentAutoDelete = Boolean.parseBoolean(
                                    properties.getOrDefault(AUTO_DELETE, "false"));
                            boolean currentInternal = Boolean.parseBoolean(
                                    properties.getOrDefault(INTERNAL, "false"));
                            amqpExchange = new PersistentExchange(exchangeName, properties,
                                    AmqpExchange.Type.value(currentType), persistentTopic, currentDurable,
                                    currentAutoDelete, currentInternal, currentArguments, routeExecutor,
                                    config.getAmqpExchangeRouteQueueSize(), config.isAmqpMultiBundleEnable());
                        } catch (Exception e) {
                            log.error("Failed to init exchange {} in vhost {}.",
                                    exchangeName, namespaceName.getLocalName(), e);
                            amqpExchangeCompletableFuture.completeExceptionally(e);
                            removeExchangeFuture(namespaceName, exchangeName);
                            return;
                        }
                        amqpExchangeCompletableFuture.complete(amqpExchange);
                    }
                }
            });
        }
        return amqpExchangeCompletableFuture;
    }

    private boolean exchangeDeclareCheck(CompletableFuture<AmqpExchange> exchangeFuture, String vhost,
                                         String exchangeName, String exchangeType, boolean durable, boolean autoDelete,
                                         Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return true;
        }

        String replyTextFormat = "PRECONDITION_FAILED - inequivalent arg '%s' for exchange '" + exchangeName + "' in "
                + "vhost '" + vhost + "': received '%s' but current is '%s'";
        String currentType = properties.get(TYPE);
        if (!StringUtils.equals(properties.get(TYPE), exchangeType)) {
            exchangeFuture.completeExceptionally(new AoPException(ErrorCodes.IN_USE,
                    String.format(replyTextFormat, "type", exchangeType, currentType), true, false));
            return false;
        }

        if (properties.containsKey(DURABLE)) {
            boolean currentDurable = Boolean.parseBoolean(properties.get(DURABLE));
            if (durable != currentDurable) {
                exchangeFuture.completeExceptionally(new AoPException(ErrorCodes.IN_USE,
                        String.format(replyTextFormat, "durable", durable, currentDurable), true, false));
                return false;
            }
        }

        if (properties.containsKey(AUTO_DELETE)) {
            boolean currentAutoDelete = Boolean.parseBoolean(properties.get(AUTO_DELETE));
            if (autoDelete != currentAutoDelete) {
                exchangeFuture.completeExceptionally(new AoPException(ErrorCodes.IN_USE,
                        String.format(replyTextFormat, "auto_delete", autoDelete, currentAutoDelete), true, false));
                return false;
            }
        }

        return true;
    }

    /**
     * Delete the exchange by namespace and exchange name.
     *
     * @param namespaceName namespace name in pulsar
     * @param exchangeName  name of exchange
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
