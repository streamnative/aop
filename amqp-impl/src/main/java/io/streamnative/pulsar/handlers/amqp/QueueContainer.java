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

import com.fasterxml.jackson.core.JsonProcessingException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
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

    protected QueueContainer(AmqpTopicManager amqpTopicManager, PulsarService pulsarService,
                             ExchangeContainer exchangeContainer) {
        this.amqpTopicManager = amqpTopicManager;
        this.pulsarService = pulsarService;
        this.exchangeContainer = exchangeContainer;
    }

    @Getter
    private Map<NamespaceName, Map<String, CompletableFuture<AmqpQueue>>> queueMap = new ConcurrentHashMap<>();

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
        queueMap.putIfAbsent(namespaceName, new ConcurrentHashMap<>());
        CompletableFuture<AmqpQueue> existingAmqpExchangeFuture = queueMap.get(namespaceName).
                putIfAbsent(queueName, queueCompletableFuture);
        if (existingAmqpExchangeFuture != null) {
            return existingAmqpExchangeFuture;
        } else {
            String topicName = PersistentQueue.getQueueTopicName(namespaceName, queueName);
            CompletableFuture<Topic> topicCompletableFuture =
                    amqpTopicManager.getTopic(topicName, createIfMissing);
            topicCompletableFuture.whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to get topic from amqpTopicManager", throwable);
                    queueMap.get(namespaceName).remove(queueName).completeExceptionally(throwable);
                } else {
                    if (null == topic) {
                        log.info("Queue topic not existed, queueName:{}", queueName);
                        queueMap.get(namespaceName).remove(queueName).complete(null);
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
                        } catch (Exception e) {
                            log.error("Failed to recover routers for queue.", e);
                            queueCompletableFuture.completeExceptionally(e);
                        }
                        queueCompletableFuture.complete(amqpQueue);
                    }
                }
            });
        }
        return queueCompletableFuture;
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
