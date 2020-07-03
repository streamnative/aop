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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Maps;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
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
 * Container for all queues in the broker.
 */
@Slf4j
public class QueueContainer {

    private AmqpTopicManager amqpTopicManager;
    private PulsarService pulsarService;
    private ExchangeContainer exchangeContainer;
    private OrderedExecutor orderedExecutor;

    protected QueueContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService,
                             ExchangeContainer exchangeContainer) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
        this.exchangeContainer = exchangeContainer;
        this.orderedExecutor = OrderedExecutor.newBuilder()
                // TODO make it configurable
                .numThreads(100)
                .name("exchange-container-workers")
                .build();
    }

    @Getter
    private Map<NamespaceName, Map<String, AmqpQueue>> queueMap = new ConcurrentHashMap<>();

    private void putQueue(NamespaceName namespaceName, String queueName, AmqpQueue amqpQueue) {
        queueMap.compute(namespaceName, (ns, map) -> {
            Map<String, AmqpQueue> amqpQueueMap = map;
            if (amqpQueueMap == null) {
                amqpQueueMap = Maps.newConcurrentMap();
            }
            amqpQueueMap.put(queueName, amqpQueue);
            return amqpQueueMap;
        });
    }

    /**
     * Get or create queue.
     *
     * @param namespaceName namespace in pulsar
     * @param queueName name of queue
     * @param createIfMissing true to create the queue if not existed
     *                        false to get the queue and return null if not existed
     * @return the completableFuture of get result
     */
    public CompletableFuture<AmqpQueue> asyncGetQueue(NamespaceName namespaceName, String queueName,
                                                      boolean createIfMissing) {
        CompletableFuture<AmqpQueue> queueCompletableFuture = new CompletableFuture<>();
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            log.error("Parameter error, namespaceName or queueName is empty.");
            queueCompletableFuture.completeExceptionally(
                    new IllegalArgumentException("NamespaceName or queueName is empty"));
            return queueCompletableFuture;
        }
        if (pulsarService.getState() != PulsarService.State.Started) {
            log.error("Pulsar service not started.");
            queueCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService not start"));
            return queueCompletableFuture;
        }
        Map<String, AmqpQueue> map = queueMap.getOrDefault(namespaceName, null);
        if (map == null || map.getOrDefault(queueName, null) == null) {
            String topicName = PersistentQueue.getQueueTopicName(namespaceName, queueName);
            orderedExecutor.executeOrdered(topicName, safeRun(() -> {
                Map<String, AmqpQueue> innerMap = queueMap.getOrDefault(namespaceName, null);
                if (innerMap == null || innerMap.getOrDefault(queueName, null) == null) {
                    CompletableFuture<Topic> topicCompletableFuture =
                            amqpTopicManager.getTopic(topicName, createIfMissing);
                    topicCompletableFuture.whenComplete((topic, throwable) -> {
                        if (throwable != null) {
                            log.error("Failed to get topic from amqpTopicManager", throwable);
                            queueCompletableFuture.completeExceptionally(throwable);
                        } else {
                            if (null == topic) {
                                log.info("Queue topic not existed, queueName:{}", queueName);
                                queueCompletableFuture.complete(null);
                            } else {
                                // recover metadata if existed
                                PersistentTopic persistentTopic = (PersistentTopic) topic;
                                Map<String, String> properties = persistentTopic.getManagedLedger().getProperties();

                                // TODO: reset connectionId, exclusive and autoDelete
                                PersistentQueue amqpQueue = new PersistentQueue(queueName, persistentTopic,
                                        0, false, false);
                                try {
                                    amqpQueue.recoverRoutersFromQueueProperties(properties, exchangeContainer,
                                            namespaceName);
                                } catch (JsonProcessingException e) {
                                    log.error("Json decode error in queue recover from properties", e);
                                    queueCompletableFuture.completeExceptionally(e);
                                }
                                putQueue(namespaceName, queueName, amqpQueue);
                                queueCompletableFuture.complete(amqpQueue);
                            }
                        }
                    });
                } else {
                    queueCompletableFuture.complete(innerMap.getOrDefault(queueName, null));
                }
            }));
        } else {
            queueCompletableFuture.complete(map.getOrDefault(queueName, null));
        }

        return queueCompletableFuture;
    }

    public AmqpQueue getQueue(NamespaceName namespaceName, String queueName) {
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            return null;
        }
        Map<String, AmqpQueue> map = queueMap.getOrDefault(namespaceName, null);
        if (map == null) {
            return null;
        }
        return map.getOrDefault(queueName, null);
    }

    /**
     * Delete the queue by namespace and exchange name.
     *
     * @param namespaceName namespace name in pulsar
     * @param queueName name of queue
     */
    public void deleteQueue(NamespaceName namespaceName, String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            return;
        }
        if (queueMap.containsKey(namespaceName)) {
            queueMap.get(namespaceName).remove(queueName);
        }
    }

}
