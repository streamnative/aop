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
package io.streamnative.pulsar.handlers.amqp.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpQueue;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueueProperties;
import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import io.streamnative.pulsar.handlers.amqp.MessagePublishContext;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;

/**
 * Persistent queue.
 */
@Slf4j
public class PersistentQueue extends AbstractAmqpQueue {
    public static final String QUEUE = "QUEUE";
    public static final String ROUTERS = "ROUTERS";
    public static final String TOPIC_PREFIX = "__amqp_queue__";

    @Getter
    private PersistentTopic indexTopic;

    private ObjectMapper jsonMapper = ObjectMapperFactory.create();

    public PersistentQueue(String queueName, PersistentTopic indexTopic, long connectionId,
                           boolean exclusive, boolean autoDelete) {
        super(queueName, true, connectionId, exclusive, autoDelete);
        this.indexTopic = indexTopic;
    }

    @Override
    public CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId) {
        CompletableFuture<Void> completableFuture = CompletableFuture.completedFuture(null);
        try {
            IndexMessage indexMessage = IndexMessage.create(exchangeName, ledgerId, entryId);
            MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(indexMessage);
            MessagePublishContext.publishMessages(message, indexTopic, new CompletableFuture<>());
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId) {
        return getRouter(exchangeName).getExchange().readEntryAsync(getName(), ledgerId, entryId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId) {
        return getRouter(exchangeName).getExchange().markDeleteAsync(getName(), ledgerId, entryId);
    }

    @Override
    public void bindExchange(AmqpExchange exchange, AmqpMessageRouter router, String bindingKey,
                             Map<String, Object> arguments) {
        super.bindExchange(exchange, router, bindingKey, arguments);
        updateQueueProperties();
    }

    @Override
    public void unbindExchange(AmqpExchange exchange) {
        super.unbindExchange(exchange);
        updateQueueProperties();
    }

    private void updateQueueProperties() {
        Map<String, String> properties = new HashMap<>();
        try {
            properties.put(ROUTERS, jsonMapper.writeValueAsString(getQueueProperties(routers)));
            properties.put(QUEUE, queueName);
        } catch (JsonProcessingException e) {
            log.error("[{}] covert map of routers to String error: {}", queueName, e.getMessage());
            return;
        }
        PulsarTopicMetadataUtils.updateMetaData(this.indexTopic, properties, queueName);
    }

    public static String getQueueTopicName(NamespaceName namespaceName, String queueName) {
        return TopicName.get(TopicDomain.persistent.value(),
                namespaceName, TOPIC_PREFIX + queueName).toString();
    }

    private List<AmqpQueueProperties> getQueueProperties(Map<String, AmqpMessageRouter> routers) {
        List<AmqpQueueProperties> propertiesList = new ArrayList<>();
        for (Map.Entry<String, AmqpMessageRouter> router : routers.entrySet()) {
            AmqpQueueProperties amqpQueueProperties = new AmqpQueueProperties();

            amqpQueueProperties.setExchangeName(router.getKey());
            amqpQueueProperties.setType(router.getValue().getType());
            amqpQueueProperties.setArguments(router.getValue().getArguments());
            amqpQueueProperties.setBindingKeys(router.getValue().getBindingKey());

            propertiesList.add(amqpQueueProperties);
        }
        return propertiesList;
    }


}
