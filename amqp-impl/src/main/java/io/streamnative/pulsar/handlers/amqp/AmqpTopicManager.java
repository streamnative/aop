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
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;



/**
 * Exchange topic manager.
 */
@Slf4j
public class AmqpTopicManager {

    private PulsarService pulsarService;

    private BrokerService brokerService;

    private AmqpConnection amqpConnection;

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
        LOOKUP_CACHE = new ConcurrentHashMap<>();

    // cache for topics: <topicName, persistentTopic>
    private final ConcurrentHashMap<String, CompletableFuture<PersistentTopic>> topics;
    private final ConcurrentHashMap<String, CompletableFuture<Topic>> exchangeTopics;

    @Getter
    private final ConcurrentHashMap<String, CompletableFuture<AmqpTopicCursorManager>> topicCursorManagers;

    public AmqpTopicManager(AmqpConnection amqpConnection) {
        this.amqpConnection = amqpConnection;
        this.pulsarService = amqpConnection.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        topics = new ConcurrentHashMap<>();
        exchangeTopics = new ConcurrentHashMap<>();
        topicCursorManagers = new ConcurrentHashMap<>();
    }

    public CompletableFuture<PersistentTopic> getTopic(String topicName) {
        CompletableFuture<PersistentTopic> topicCompletableFuture = new CompletableFuture<>();

        return topics.computeIfAbsent(topicName,
            t -> {
                getTopicBroker(t).whenCompleteAsync((ignore, th) -> {
                    if (th != null || ignore == null) {
                        log.warn("Failed getTopicBroker {}, return null PersistentTopic. throwable: ", t, th);

                        // get topic broker returns null. topic should be removed from LookupCache.
                        if (ignore == null) {
                            removeLookupCache(topicName);
                        }

                        topicCompletableFuture.complete(null);
                        return;
                    }

                    if (log.isDebugEnabled()) {
                        log.debug("GetTopicBroker for {} in ExchangeTopicManager. brokerAddress: {}", t, ignore);
                    }

                    brokerService
                        .getTopic(t, true)
                        .whenComplete((t2, throwable) -> {
                            if (throwable != null) {
                                log.error("Failed to getTopic {}. exception: {}", t, throwable);
                                // failed to getTopic from current broker, remove cache, which added in getTopicBroker.
                                removeLookupCache(t);
                                topicCompletableFuture.complete(null);
                                return;
                            }

                            try {
                                if (t2.isPresent()) {
                                    PersistentTopic persistentTopic = (PersistentTopic) t2.get();
                                    topicCompletableFuture.complete(persistentTopic);
                                } else {
                                    log.error("Get empty topic for name {}", t);
                                    topicCompletableFuture.complete(null);
                                }
                            } catch (Exception e) {
                                log.error("Failed to get client in registerInPersistentTopic {}. exception:", t, e);
                                topicCompletableFuture.complete(null);
                            }
                        });
                });
                return topicCompletableFuture;
            });
    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {

        return LOOKUP_CACHE.computeIfAbsent(topicName, t -> {
            if (log.isDebugEnabled()) {
                log.debug("Topic {} not in Lookup_cache, call lookupBroker", topicName);
            }
            CompletableFuture<InetSocketAddress> returnFuture = new CompletableFuture<>();
            Backoff backoff = new Backoff(
                100, TimeUnit.MILLISECONDS,
                30, TimeUnit.SECONDS,
                30, TimeUnit.SECONDS
            );
            lookupBroker(topicName, backoff, returnFuture);
            return returnFuture;
        });
    }

    // this method do the real lookup into Pulsar broker.
    // retFuture will be completed with null when meet error.
    private void lookupBroker(String topicName,
        Backoff backoff,
        CompletableFuture<InetSocketAddress> retFuture) {
        try {
            ((PulsarClientImpl) pulsarService.getClient()).getLookup()
                .getBroker(TopicName.get(topicName))
                .thenAccept(pair -> {
                    checkState(pair.getLeft().equals(pair.getRight()));
                    retFuture.complete(pair.getLeft());
                })
                .exceptionally(th -> {
                    long waitTimeMs = backoff.next();

                    if (backoff.isMandatoryStopMade()) {
                        log.warn("GetBroker for topic {} failed, retried too many times, waitTimeMs: {},"
                            + " return null. throwable: {}", topicName, waitTimeMs, th);
                        retFuture.complete(null);
                    } else {
                        log.warn("[{}] getBroker for topic failed, will retry in {} ms. throwable: {}",
                            topicName, waitTimeMs, th);
                        pulsarService.getExecutor()
                            .schedule(() -> lookupBroker(topicName, backoff, retFuture),
                                waitTimeMs,
                                TimeUnit.MILLISECONDS);
                    }
                    return null;
                });
        } catch (PulsarServerException e) {
            log.error("GetTopicBroker for topic {} failed get pulsar client, return null. throwable: ", topicName, e);
            retFuture.complete(null);
        }
    }

    public static void removeLookupCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
    }

    public Topic getOrCreateTopic(String topicName, boolean createIfMissing) {
        return getTopic(topicName, createIfMissing).join();
    }

    public CompletableFuture<Topic> getTopic(String topicName, boolean createIfMissing) {

        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
        return exchangeTopics.computeIfAbsent(topicName,
            t -> {
                // setup ownership of service unit to this broker
                pulsarService.getNamespaceService().getBrokerServiceUrlAsync(TopicName.get(topicName), true).
                    whenComplete((addr, th) -> {
                        if (th != null || addr == null || addr.get() == null) {
                            log.warn("Failed getBrokerServiceUrl {}, return null Topic. throwable: ", t, th);
                            topicCompletableFuture.complete(null);
                            return;
                        }
                        if (log.isDebugEnabled()) {
                            log.debug("getBrokerServiceUrl for {} in ExchangeTopicManager. brokerAddress: {}",
                                t, addr.get().getLookupData().getBrokerUrl());
                        }
                        brokerService.getTopic(t, createIfMissing)
                            .whenComplete((topicOptional, throwable) -> {
                                if (throwable != null) {
                                    log.error("Failed to getTopic {}. exception: {}", t, throwable);
                                    topicCompletableFuture.complete(null);
                                    return;
                                }
                                try {
                                    if (topicOptional.isPresent()) {
                                        Topic topic = topicOptional.get();
                                        topicCompletableFuture.complete(topic);
                                    } else {
                                        log.error("Get empty topic for name {}", t);
                                        topicCompletableFuture.complete(null);
                                    }
                                } catch (Exception e) {
                                    log.error("Failed to get client in registerInPersistentTopic {}. "
                                        + "exception:", t, e);
                                    topicCompletableFuture.complete(null);
                                }
                            });
                    });
                return topicCompletableFuture;
            });
    }

    public void deleteTopic(String topicName) {

        if (null != exchangeTopics.get(topicName)){
            exchangeTopics.remove(topicName);
        }

    }

    public CompletableFuture<AmqpTopicCursorManager> getTopicCursorManager(String topicName) {
        return topicCursorManagers.computeIfAbsent(
            topicName,
            t -> {
                CompletableFuture<PersistentTopic> topic = getTopic(t);
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
