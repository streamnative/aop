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
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
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
 * Container for all queues in the broker.
 */
@Slf4j
public class QueueContainer {
    private static Executor executor = Executors.newCachedThreadPool();

    @Getter
    private static Map<NamespaceName, Map<String, AmqpQueue>> queueMap = new ConcurrentHashMap<>();

    public static void putQueue(NamespaceName namespaceName, String queueName, AmqpQueue amqpQueue) {
        queueMap.compute(namespaceName, (ns, map) -> {
            Map<String, AmqpQueue> amqpQueueMap = map;
            if (amqpQueueMap == null) {
                amqpQueueMap = Maps.newConcurrentMap();
            }
            amqpQueueMap.put(queueName, amqpQueue);
            return amqpQueueMap;
        });
    }

    public static CompletableFuture<AmqpQueue> asyncGetQueue(PulsarService pulsarService, NamespaceName namespaceName,
                                                             String queueName, boolean createIfMissing) {
        CompletableFuture<AmqpQueue> queueCompletableFuture = new CompletableFuture<>();
        executor.execute(() -> {
            if (namespaceName == null || StringUtils.isEmpty(queueName)) {
                queueCompletableFuture.complete(null);
            }
            Map<String, AmqpQueue> map = queueMap.getOrDefault(namespaceName, null);
            if (map == null || map.getOrDefault(queueName, null) == null) {
                // check pulsar topic
                if (pulsarService.getState() != PulsarService.State.Started) {
                    queueCompletableFuture.completeExceptionally(
                            new PulsarServerException("PulsarService not start"));
                }
                String topicName = PersistentQueue.getQueueTopicName(namespaceName, queueName);
                CompletableFuture<Topic> topicCompletableFuture =
                        AmqpTopicManager.getTopic(pulsarService, topicName, createIfMissing);
                topicCompletableFuture.whenComplete((topic, throwable) -> {
                    if (throwable != null) {
                        log.error("Get topic error:{}", throwable.getMessage());
                        queueCompletableFuture.complete(null);
                    } else {
                        if (null == topic) {
                            log.error("Queue topic not existed, queueName:{}", queueName);
                            queueCompletableFuture.complete(null);
                        } else {
                            // recover metadata if existed
                            PersistentTopic persistentTopic = (PersistentTopic) topic;
                            Map<String, String> properties = persistentTopic.getManagedLedger().getProperties();

                            PersistentQueue amqpQueue = new PersistentQueue(queueName, persistentTopic,
                                0, false, false);
                            amqpQueue.recoverRoutersFromQueueProperties(properties, pulsarService, namespaceName);
                            QueueContainer.putQueue(namespaceName, queueName, amqpQueue);
                            queueCompletableFuture.complete(amqpQueue);
                        }
                    }
                });

            } else {
                queueCompletableFuture.complete(map.getOrDefault(queueName, null));
            }
        });
        return queueCompletableFuture;
    }

    public static AmqpQueue getQueue(NamespaceName namespaceName, String queueName) {
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            return null;
        }
        Map<String, AmqpQueue> map = queueMap.getOrDefault(namespaceName, null);
        if (map == null) {
            return null;
        }
        return map.getOrDefault(queueName, null);
    }

    public static void deleteQueue(NamespaceName namespaceName, String queueName) {
        if (StringUtils.isEmpty(queueName)) {
            return;
        }
        if (queueMap.containsKey(namespaceName)) {
            queueMap.get(namespaceName).remove(queueName);
        }
    }

}
