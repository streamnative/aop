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
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;


/**
 * Exchange topic manager.
 */
@Slf4j
public class ExchangeTopicManager {

    private PulsarService pulsarService;

    private BrokerService brokerService;

    private AmqpConnection amqpConnection;

    // cache for topics: <topicName, persistentTopic>
    // TODO change the MockTopic to PersistentTopic
    private final ConcurrentHashMap<String, CompletableFuture<MockTopic>> topics;

    public ExchangeTopicManager(AmqpConnection amqpConnection) {
        this.amqpConnection = amqpConnection;
        this.pulsarService = amqpConnection.getPulsarService();
        this.brokerService = pulsarService.getBrokerService();
        topics = new ConcurrentHashMap<>();
    }

    public CompletableFuture<MockTopic> getTopic(String exchangeName) {
        CompletableFuture<MockTopic> topicCompletableFuture = new CompletableFuture<>();
        return topics.computeIfAbsent(exchangeName, t -> {
            MockTopic mockTopic = null;
            mockTopic = new MockTopic();
            topicCompletableFuture.complete(mockTopic);
            return topicCompletableFuture;
        });
    }

}
