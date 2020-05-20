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

import com.fasterxml.jackson.databind.JavaType;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Container for all exchanges in the broker.
 */
@Slf4j
public class ExchangeContainer extends TopicContainer {

    @Getter
    private final static Map<NamespaceName, Map<String, AmqpExchange>> exchangeMap = new ConcurrentHashMap<>();

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
        AmqpExchange exchange = map.getOrDefault(exchangeName, null);
        if (exchange == null) {
            return tryRecover(namespaceName, exchangeName);
        }
        return exchange;
    }

    public static void deleteExchange(String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        exchangeMap.remove(exchangeName);
    }

    public static AmqpExchange tryRecover( NamespaceName namespaceName, String exchangeName) {
        try {
            TopicName topicName = TopicName.get(TopicDomain.persistent.value(), namespaceName, exchangeName);
            Topic topic = amqpTopicManager.getOrCreateTopic(topicName.toString(), true);
            if (!(topic instanceof PersistentTopic)) {
                return null;
            }
            String mlName = ((PersistentTopic) topic).getManagedLedger().getName();
            ManagedLedgerInfo exchangeManagedLegerInfo =
                    pulsarService.getManagedLedgerFactory().getManagedLedgerInfo(mlName);
            Map<String, String> exchangeProperties = exchangeManagedLegerInfo.properties;
            if (exchangeProperties == null) {
                return null;
            }
            String exchangeStr = exchangeProperties.get(PersistentExchange.EXCHANGE);
            String typeStr = exchangeProperties.get(PersistentExchange.TYPE);
            String queuesStr = exchangeProperties.get(PersistentExchange.QUEUES);

            PersistentExchange persistentExchange =
                    new PersistentExchange(exchangeName, PersistentExchange.Type.value(typeStr),
                            (PersistentTopic) topic, amqpTopicManager, false);
            putExchange(namespaceName, exchangeName, persistentExchange);

            JavaType javaType = jsonMapper.getTypeFactory().constructParametricType(List.class, String.class);
            List<String> queueList = jsonMapper.readValue(queuesStr, javaType);
            for (String queueName : queueList) {
                ManagedLedgerInfo queueManagedLedgerInfo =
                        pulsarService.getManagedLedgerFactory().getManagedLedgerInfo(
                                PersistentQueue.getIndexTopicName(namespaceName, queueName));
                Map<String, String> queueProperties = queueManagedLedgerInfo.properties;
                String queueStr = queueProperties.get(PersistentQueue.QUEUE);
                String routesStr = queueProperties.get(PersistentQueue.ROUTERS);

                Topic indexTopic = amqpTopicManager.getOrCreateTopic(
                        PersistentQueue.getIndexTopicName(namespaceName, queueName), true);

                JavaType queuePropertiesType =
                        jsonMapper.getTypeFactory().constructParametricType(List.class, AmqpQueueProperties.class);
                List<AmqpQueueProperties> amqpQueuePropertiesList =
                        jsonMapper.readValue(routesStr, queuePropertiesType);

                log.info("queueProperties: {}", queueProperties);
                PersistentQueue persistentQueue = new PersistentQueue(queueStr, (PersistentTopic) indexTopic);
                QueueContainer.putQueue(namespaceName, queueName, persistentQueue);

                AmqpMessageRouter amqpMessageRouter =
                        AbstractAmqpMessageRouter.generateRouter(AmqpExchange.Type.value(typeStr));
            }
            log.info("exchangeProperties: {}", exchangeProperties);
        } catch (Exception e) {
            log.error("Failed recover exchange. namespaceName: {}, exchangeName: {}", namespaceName, exchangeName, e);
        }
        return null;
    }
}
