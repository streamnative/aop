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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.exchange.ExchangeDefaults;


/**
 * Container for all exchanges in the broker.
 */
@Slf4j
public class ExchangeContainer {

    private static final Map<String, AmqpExchange.Type> BUILDIN_EXCHANGE_NAME_SET = new HashMap<>();
    private static PulsarService pulsarService;

    public static void init(PulsarService pulsarService) {
        if (ExchangeContainer.pulsarService == null) {
            ExchangeContainer.pulsarService = pulsarService;
            BUILDIN_EXCHANGE_NAME_SET.put(AbstractAmqpExchange.DEFAULT_EXCHANGE_DURABLE, AmqpExchange.Type.Direct);
            BUILDIN_EXCHANGE_NAME_SET.put(ExchangeDefaults.DIRECT_EXCHANGE_NAME, AmqpExchange.Type.Direct);
            BUILDIN_EXCHANGE_NAME_SET.put(ExchangeDefaults.FANOUT_EXCHANGE_NAME, AmqpExchange.Type.Fanout);
            BUILDIN_EXCHANGE_NAME_SET.put(ExchangeDefaults.TOPIC_EXCHANGE_NAME, AmqpExchange.Type.Topic);
        }
    }

    @Getter
    private static Map<NamespaceName, Map<String, AmqpExchange>> exchangeMap = new ConcurrentHashMap<>();

    public static void putExchange(NamespaceName namespaceName, String exchangeName, AmqpExchange amqpExchange) {
        exchangeMap.compute(namespaceName, (name, map) -> {
            Map<String, AmqpExchange> amqpExchangeMap = map;
            if (amqpExchangeMap == null) {
                amqpExchangeMap = Maps.newConcurrentMap();
            }
            amqpExchangeMap.put(exchangeName, amqpExchange);
            return amqpExchangeMap;
        });
    }

    public static AmqpExchange getExchange(NamespaceName namespaceName, String exchangeName) {
        if (namespaceName == null || StringUtils.isEmpty(exchangeName)) {
            return null;
        }
        Map<String, AmqpExchange> map = exchangeMap.getOrDefault(namespaceName, null);
        if (map == null) {
            return null;
        }
        return map.getOrDefault(exchangeName, null);
    }

    public static void deleteExchange(NamespaceName namespaceName, String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        if (exchangeMap.containsKey(namespaceName)) {
            exchangeMap.get(namespaceName).remove(exchangeName);
        }
    }

    public static void initBuildInExchange(NamespaceName namespaceName) {
        for (Map.Entry<String, AmqpExchange.Type> entry : BUILDIN_EXCHANGE_NAME_SET.entrySet()) {
            if (getExchange(namespaceName, entry.getKey()) == null) {
                addBuildInExchanges(namespaceName, entry.getKey(), entry.getValue());
            }
        }
    }

    private static void addBuildInExchanges(NamespaceName namespaceName,
                                            String exchangeName, AmqpExchange.Type exchangeType) {
        String topicName = PersistentExchange.getExchangeTopicName(namespaceName, exchangeName);
        AmqpTopicManager.getTopic(pulsarService, topicName, true).whenComplete((topic, throwable) -> {
            if (throwable != null) {
                log.error("Create default exchange topic failed. errorMsg: {}", throwable.getMessage(), throwable);
                return;
            }
            ExchangeContainer.putExchange(namespaceName, exchangeName,
                    new PersistentExchange(exchangeName, exchangeType,
                            (PersistentTopic) topic, false));
        });
    }

}
