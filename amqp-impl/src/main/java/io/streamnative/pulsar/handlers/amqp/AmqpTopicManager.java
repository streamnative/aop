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

import static com.google.common.base.Preconditions.checkState;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Exchange and queue topic manager.
 */
@Slf4j
public class AmqpTopicManager {

    private PulsarService pulsarService;

    private BrokerService brokerService;

    @Getter
    private final ConcurrentHashMap<String, CompletableFuture<AmqpTopicCursorManager>> topicCursorManagers;

    public AmqpTopicManager(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
        this.brokerService = pulsarService.getBrokerService();
        topicCursorManagers = new ConcurrentHashMap<>();
    }

    public Topic getOrCreateTopic(String topicName, boolean createIfMissing) {
        return getTopic(topicName, createIfMissing).join();
    }

    public CompletableFuture<Topic> getTopic(String topicName, boolean createIfMissing) {
        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
        // setup ownership of service unit to this broker
        pulsarService.getNamespaceService().getBrokerServiceUrlAsync(TopicName.get(topicName), true).
                whenComplete((addr, th) -> {
                    log.info("Find getBrokerServiceUrl {}, return null Topic.: {}", addr, topicName);
                    if (th != null || addr == null || addr.get() == null) {
                        log.warn("Failed getBrokerServiceUrl {}, return null Topic. throwable: ", topicName, th);
                        topicCompletableFuture.complete(null);
                        return;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("getBrokerServiceUrl for {} in ExchangeTopicManager. brokerAddress: {}",
                                topicName, addr.get().getLookupData().getBrokerUrl());
                    }
                    brokerService.getTopic(topicName, createIfMissing)
                            .whenComplete((topicOptional, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to getTopic {}. exception: {}", topicName, throwable);
                                    topicCompletableFuture.complete(null);
                                    return;
                                }
                                try {
                                    if (topicOptional.isPresent()) {
                                        Topic topic = topicOptional.get();
                                        topicCompletableFuture.complete(topic);
                                    } else {
                                        log.error("Get empty topic for name {}", topicName);
                                        topicCompletableFuture.complete(null);
                                    }
                                } catch (Exception e) {
                                    log.error("Failed to get client in registerInPersistentTopic {}. "
                                            + "exception:", topicName, e);
                                    topicCompletableFuture.complete(null);
                                }
                            });
                });
        return topicCompletableFuture;
    }

    public CompletableFuture<AmqpTopicCursorManager> getTopicCursorManager(String topicName) {
        return topicCursorManagers.computeIfAbsent(
            topicName,
            t -> {
                CompletableFuture<Topic> topic = getTopic(t, true);
                checkState(topic != null);

                return topic.thenApply(t2 -> {
                    if (log.isDebugEnabled()) {
                        log.debug(" Call getTopicCursorManager for {}, and create TCM for {}.",
                            topicName, t2);
                    }

                    if (t2 == null) {
                        return null;
                    }
                    // return consumer manager
                    return new AmqpTopicCursorManager((PersistentTopic) t2);
                });
            }
        );
    }
}
