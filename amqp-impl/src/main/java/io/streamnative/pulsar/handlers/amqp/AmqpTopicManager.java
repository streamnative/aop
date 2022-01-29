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

import io.streamnative.pulsar.handlers.amqp.common.exception.EmptyLookupResultException;
import io.streamnative.pulsar.handlers.amqp.common.exception.NamespaceNotFoundException;
import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Exchange and queue topic manager.
 */
@Slf4j
public class AmqpTopicManager {

    private final PulsarService pulsarService;

    private volatile Field fenceField;

    public AmqpTopicManager(PulsarService pulsarService) {
        this.pulsarService = pulsarService;
    }

    public Topic getOrCreateTopic(String topicName, boolean createIfMissing) {
        return getTopic(topicName, createIfMissing).join();
    }

    public CompletableFuture<Topic> getTopic(String topicName, boolean createIfMissing) {
        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
        if (null == pulsarService) {
            log.error("PulsarService is not set.");
            topicCompletableFuture.completeExceptionally(new PulsarServerException("PulsarService is not set."));
            return topicCompletableFuture;
        }
        final TopicName tpName = TopicName.get(topicName);
        // Check the namespace first to make sure the namespace is existing.
        pulsarService.getPulsarResources().getNamespaceResources().getPoliciesAsync(tpName.getNamespaceObject())
                .thenCompose(policies -> {
                    if (!policies.isPresent()) {
                        return FutureUtil.failedFuture(new NamespaceNotFoundException(tpName.getNamespaceObject()));
                    }
                    // setup ownership of service unit to this broker
                    return pulsarService.getNamespaceService().getBrokerServiceUrlAsync(
                            tpName, LookupOptions.builder().authoritative(true).build());
                })
                .thenCompose(lookupOp -> {
                    if (!lookupOp.isPresent()) {
                        return FutureUtil.failedFuture(new EmptyLookupResultException(tpName));
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("Get broker service url for {}. lookupResult: {}",
                                topicName, lookupOp.get().getLookupData().getBrokerUrl());
                    }
                    return pulsarService.getBrokerService().getTopic(topicName, createIfMissing);
                })
                .thenAccept(topicOp -> {
                    if (!topicOp.isPresent()) {
                        log.error("Get empty topic for name {}", topicName);
                        topicCompletableFuture.complete(null);
                        return;
                    }

                    AbstractTopic persistentTopic = (AbstractTopic) topicOp.get();
                    if (checkTopicIsFenced(persistentTopic, topicCompletableFuture)) {
                        return;
                    }

                    try {
                        persistentTopic.getHierarchyTopicPolicies().getInactiveTopicPolicies()
                                .updateTopicValue(new InactiveTopicPolicies(
                                        InactiveTopicDeleteMode.delete_when_no_subscriptions, 1000, false));
                        topicCompletableFuture.complete(persistentTopic);
                    } catch (Exception e) {
                        log.error("Failed to get client in registerInPersistentTopic {}. ", topicName, e);
                        topicCompletableFuture.complete(null);
                    }
                })
                .exceptionally(throwable -> {
                    topicCompletableFuture.completeExceptionally(throwable);
                    return null;
                });
        return topicCompletableFuture;
    }

    private boolean checkTopicIsFenced(Topic topic, CompletableFuture<Topic> topicCompletableFuture) {
        try {
            if (fenceField == null) {
                fenceField = AbstractTopic.class.getDeclaredField("isFenced");
                fenceField.setAccessible(true);
            }
            boolean isFenced = fenceField.getBoolean(topic);
            if (isFenced) {
                topicCompletableFuture.completeExceptionally(
                        new RuntimeException("The topic " + topic.getName() + " is already fenced."));
            }
            return isFenced;
        } catch (NoSuchFieldException | IllegalAccessException e) {
            topicCompletableFuture.completeExceptionally(e);
            return true;
        }
    }

}
