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

import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpQueue;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpQueueProperties;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Persistent queue.
 */
@Slf4j
public class PersistentQueue extends AbstractAmqpQueue {
    public static final String QUEUE = "QUEUE";
    public static final String ROUTERS = "ROUTERS";
    public static final String TOPIC_PREFIX = "__amqp_queue__";
    public static final String DEFAULT_SUBSCRIPTION = "defaultSubscription";

    @Getter
    private PersistentTopic indexTopic;

    private ObjectMapper jsonMapper;

    private AmqpEntryWriter amqpEntryWriter;

    private final long exchangeClearTaskInterval;
    private PositionImpl checkpointLac;
    private final Map<String, Position> checkpointExchangeRoutePos = new ConcurrentHashMap<>();

    public PersistentQueue(String queueName, PersistentTopic indexTopic,
                           long connectionId,
                           boolean exclusive, boolean autoDelete,
                           long exchangeClearTaskInterval) {
        super(queueName, true, connectionId, exclusive, autoDelete);
        this.indexTopic = indexTopic;
        topicNameValidate();
        this.jsonMapper = new ObjectMapper();
        this.amqpEntryWriter = new AmqpEntryWriter(indexTopic);
        this.exchangeClearTaskInterval = exchangeClearTaskInterval;
        scheduleExchangeClearTask();
    }

    @Override
    public CompletableFuture<Void> writeIndexMessageAsync(String exchangeName, long ledgerId, long entryId,
                                                          Map<String, Object> properties) {
        try {
            IndexMessage indexMessage = IndexMessage.create(exchangeName, ledgerId, entryId, properties);
            MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(indexMessage);
            return amqpEntryWriter.publishMessage(message).thenApply(__ -> null);
        } catch (Exception e) {
            log.error("Failed to writer index message for exchange {} with position {}:{}.",
                    exchangeName, ledgerId, entryId);
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Entry> readEntryAsync(String exchangeName, long ledgerId, long entryId) {
        return getRouter(exchangeName).getExchange().readEntryAsync(getName(), ledgerId, entryId);
    }

    @Override
    public CompletableFuture<Void> acknowledgeAsync(String exchangeName, long ledgerId, long entryId) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> bindExchange(AmqpExchange exchange, AmqpMessageRouter router, String bindingKey,
                             Map<String, Object> arguments) {
        return super.bindExchange(exchange, router, bindingKey, arguments).thenApply(__ -> {
            updateQueueProperties();
            return null;
        });
    }

    @Override
    public void unbindExchange(AmqpExchange exchange) {
        super.unbindExchange(exchange);
        updateQueueProperties();
    }

    @Override
    public Topic getTopic() {
        return indexTopic;
    }

    public void recoverRoutersFromQueueProperties(Map<String, String> properties,
                                                  ExchangeContainer exchangeContainer,
                                                  NamespaceName namespaceName) throws JsonProcessingException {
        if (null == properties || properties.isEmpty() || !properties.containsKey(ROUTERS)) {
            return;
        }
        List<AmqpQueueProperties> amqpQueueProperties = jsonMapper.readValue(properties.get(ROUTERS),
                new TypeReference<List<AmqpQueueProperties>>() {
                });
        if (amqpQueueProperties == null) {
            return;
        }
        amqpQueueProperties.stream().forEach((amqpQueueProperty) -> {
            // recover exchange
            String exchangeName = amqpQueueProperty.getExchangeName();
            Set<String> bindingKeys = amqpQueueProperty.getBindingKeys();
            Map<String, Object> arguments = amqpQueueProperty.getArguments();
            CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                    exchangeContainer.asyncGetExchange(namespaceName, exchangeName, false, null);
            amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
                AmqpMessageRouter messageRouter = AbstractAmqpMessageRouter.
                        generateRouter(AmqpExchange.Type.value(amqpQueueProperty.getType().toString()));
                messageRouter.setQueue(this);
                messageRouter.setExchange(amqpExchange);
                messageRouter.setArguments(arguments);
                messageRouter.setBindingKeys(bindingKeys);
                amqpExchange.addQueue(this).thenAccept(__ -> routers.put(exchangeName, messageRouter));
            });
        });
    }

    private void updateQueueProperties() {
        Map<String, String> properties = new HashMap<>();
        try {
            properties.put(ROUTERS, jsonMapper.writeValueAsString(getQueueProperties(routers)));
            properties.put(QUEUE, queueName);
        } catch (JsonProcessingException e) {
            log.error("[{}] Failed to covert map of routers to String", queueName, e);
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

    private void topicNameValidate() {
        String[] nameArr = this.indexTopic.getName().split("/");
        checkArgument(nameArr[nameArr.length - 1].equals(TOPIC_PREFIX + queueName),
                "The queue topic name does not conform to the rules(%s%s).",
                TOPIC_PREFIX, "exchangeName");
    }

    private void scheduleExchangeClearTask() {
        this.indexTopic.getBrokerService().getPulsar().getExecutor()
                .schedule(this::exchangeClearCheckpointTask, exchangeClearTaskInterval, TimeUnit.MILLISECONDS);
    }

    private synchronized void exchangeClearCheckpointTask() {
        this.checkpointLac = null;
        Collection<CompletableFuture<Void>> futures = new ArrayList<>();
        for (AmqpMessageRouter router : routers.values()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            ((PersistentTopic) router.getExchange().getTopic()).getManagedLedger()
                    .asyncOpenCursor("__amqp_replicator__" + router.getExchange().getName(), new AsyncCallbacks.OpenCursorCallback() {
                @Override
                public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                    PositionImpl pos = (PositionImpl) cursor.getMarkDeletedPosition();
                    checkpointExchangeRoutePos.put(router.getExchange().getName(), pos);
                    future.complete(null);
                }

                @Override
                public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                    log.warn("Failed to get exchange route position for queue {}.", queueName, exception);
                    future.completeExceptionally(exception);
                }
            }, null);
            futures.add(future);
        }
        FutureUtil.waitForAll(futures).whenComplete((__, t) -> {
            if (t != null) {
                scheduleExchangeClearTask();
                return;
            }
            this.checkpointLac = (PositionImpl) indexTopic.getLastPosition();
            exchangeCleanup();
        });
    }

    private void exchangeCleanup() {
        PositionImpl indexMarkDeletePos = (PositionImpl) indexTopic.getSubscription(DEFAULT_SUBSCRIPTION)
                .getCursor().getMarkDeletedPosition();
        Collection<CompletableFuture<Void>> futures = new ArrayList<>();
        if (indexMarkDeletePos.compareTo(this.checkpointLac) >= 0) {
            for (AmqpMessageRouter router : routers.values()) {
                Position position = checkpointExchangeRoutePos.get(router.getExchange().getName());
                if (position != null) {
                    futures.add(router.getExchange().markDeleteAsync(
                            queueName, position.getLedgerId(), position.getEntryId()));
                }
            }
            FutureUtil.waitForAll(futures).thenRun(this::scheduleExchangeClearTask);
        } else {
            this.indexTopic.getBrokerService().getPulsar().getExecutor().schedule(
                    this::exchangeCleanup, exchangeClearTaskInterval, TimeUnit.MILLISECONDS);
        }
    }

}
