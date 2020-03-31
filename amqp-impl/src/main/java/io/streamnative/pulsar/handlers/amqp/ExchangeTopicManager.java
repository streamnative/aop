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

import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.Backoff;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;

import static com.google.common.base.Preconditions.checkState;


/**
 * Exchange topic manager.
 */
@Slf4j
public class ExchangeTopicManager {

    private PulsarService pulsarService;

    private BrokerService brokerService;

    private AmqpConnection amqpConnection;

    public static final ConcurrentHashMap<String, CompletableFuture<InetSocketAddress>>
            LOOKUP_CACHE = new ConcurrentHashMap<>();

    // cache for topics: <topicName, persistentTopic>
    // TODO change the MockTopic to PersistentTopic
    private final ConcurrentHashMap<String, CompletableFuture<Topic>> topics;

    public ExchangeTopicManager(AmqpConnection amqpConnection) {
        this.amqpConnection = amqpConnection;
        this.pulsarService = amqpConnection.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        topics = new ConcurrentHashMap<>();
    }

    public CompletableFuture<Topic> getTopic(String topicName) {
        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
        return topics.computeIfAbsent(topicName, t -> {
            brokerService.getTopic(topicName, true).whenComplete((topic, throwable) -> {
                if (throwable != null) {
                    topicCompletableFuture.completeExceptionally(throwable);
                }
                if (topic.isPresent()) {
                    topicCompletableFuture.complete(topic.get());
                } else {
                    topicCompletableFuture.complete(null);
                }
            });
            return topicCompletableFuture;
        });
    }

//    public CompletableFuture<Topic> getTopic(String topicName) {
//        CompletableFuture<Topic> topicCompletableFuture = new CompletableFuture<>();
//
//        int channelId = 1;
//
//        return topics.computeIfAbsent(topicName,
//                t -> {
//                    getTopicBroker(t).whenCompleteAsync((ignore, th) -> {
//                        if (th != null || ignore == null) {
//                            log.warn("[{}] Failed getTopicBroker {}, return null PersistentTopic. throwable: ",
//                                    1, t, th);
//
//                            // get topic broker returns null. topic should be removed from LookupCache.
//                            if (ignore == null) {
//                                removeLookupCache(topicName);
//                            }
//
//                            topicCompletableFuture.complete(null);
//                            return;
//                        }
//
//                        if (log.isDebugEnabled()) {
//                            log.debug("[{}] getTopicBroker for {} in KafkaTopicManager. brokerAddress: {}",
//                                    channelId, t, ignore);
//                        }
//
//                        brokerService
//                                .getTopic(t, true)
//                                .whenComplete((t2, throwable) -> {
//                                    if (throwable != null) {
//                                        log.error("[{}] Failed to getTopic {}. exception:",
//                                                channelId, t, throwable);
//                                        // failed to getTopic from current broker, remove cache, which added in getTopicBroker.
//                                        removeLookupCache(t);
//                                        topicCompletableFuture.complete(null);
//                                        return;
//                                    }
//
//                                    try {
//                                        if (t2.isPresent()) {
//                                            PersistentTopic persistentTopic = (PersistentTopic) t2.get();
//                                            topicCompletableFuture.complete(persistentTopic);
//                                        } else {
//                                            log.error("[{}]Get empty topic for name {}",
//                                                    channelId, t);
//                                            topicCompletableFuture.complete(null);
//                                        }
//                                    } catch (Exception e) {
//                                        log.error("[{}] Failed to get client in registerInPersistentTopic {}. exception:",
//                                                channelId, t, e);
//                                        topicCompletableFuture.complete(null);
//                                    }
//                                });
//                    });
//                    return topicCompletableFuture;
//                });
//    }

    // call pulsarclient.lookup.getbroker to get and own a topic.
    // when error happens, the returned future will complete with null.
    public CompletableFuture<InetSocketAddress> getTopicBroker(String topicName) {

        return LOOKUP_CACHE.computeIfAbsent(topicName, t -> {
            if (log.isDebugEnabled()) {
                log.debug("[{}] topic {} not in Lookup_cache, call lookupBroker",
                        1, topicName);
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
                            log.warn("[{}] getBroker for topic failed, retried too many times, return null. throwable: ",
                                    1, topicName, waitTimeMs, th);
                            retFuture.complete(null);
                        } else {
                            log.warn("[{}] getBroker for topic failed, will retry in {} ms. throwable: ",
                                    topicName, waitTimeMs, th);
                            pulsarService.getExecutor()
                                    .schedule(() -> lookupBroker(topicName, backoff, retFuture),
                                            waitTimeMs,
                                            TimeUnit.MILLISECONDS);
                        }
                        return null;
                    });
        } catch (PulsarServerException e) {
            log.error("[{}] getTopicBroker for topic {} failed get pulsar client, return null. throwable: ",
                    1, topicName, e);
            retFuture.complete(null);
        }
    }

    public static void removeLookupCache(String topicName) {
        LOOKUP_CACHE.remove(topicName);
    }

    public Topic getOrCreateTopic(String exchangeName, boolean createIfMissing) {
        return amqpConnection.getPulsarService().getBrokerService().
                getTopic(exchangeName, createIfMissing).thenApply(Optional::get).join();
    }

}
