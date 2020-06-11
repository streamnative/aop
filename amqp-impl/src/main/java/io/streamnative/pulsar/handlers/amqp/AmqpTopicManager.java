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

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Exchange and queue topic manager.
 */
@Slf4j
public class AmqpTopicManager {


    public static Topic getOrCreateTopic(PulsarService pulsarService, String topicName, boolean createIfMissing) {
        return getTopic(pulsarService, topicName, createIfMissing).join();
    }

    public static CompletableFuture<Topic> getTopic(PulsarService pulsarService, String topicName,
                                                    boolean createIfMissing) {
        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
        if (null == pulsarService) {
            log.error("PulsarService is not set.");
            topicCompletableFuture.completeExceptionally(new Exception("PulsarService is not set."));
            return topicCompletableFuture;
        }
        // setup ownership of service unit to this broker
        pulsarService.getNamespaceService().getBrokerServiceUrlAsync(TopicName.get(topicName), true).
                whenComplete((addr, th) -> {
                    log.info("Find getBrokerServiceUrl {}, return Topic: {}", addr, topicName);
                    if (th != null || addr == null || addr.get() == null) {
                        log.warn("Failed getBrokerServiceUrl {}, return null Topic. throwable: ", topicName, th);
                        topicCompletableFuture.complete(null);
                        return;
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("getBrokerServiceUrl for {} in ExchangeTopicManager. brokerAddress: {}",
                                topicName, addr.get().getLookupData().getBrokerUrl());
                    }
                    pulsarService.getBrokerService().getTopic(topicName, createIfMissing)
                            .whenComplete((topicOptional, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to getTopic {}. exception: {}", topicName, throwable);
                                    topicCompletableFuture.complete(null);
                                    return;
                                }
                                try {
                                    if (topicOptional.isPresent()) {
                                        Topic topic = topicOptional.get();
                                        PersistentTopic persistentTopic = (PersistentTopic) topic;
                                        persistentTopic.setDeleteWhileInactive(false);
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

}
