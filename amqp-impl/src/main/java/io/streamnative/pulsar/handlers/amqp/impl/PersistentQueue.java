/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.impl;

import static io.streamnative.pulsar.handlers.amqp.utils.TopicUtil.getTopicName;
import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.FALSE;
import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.TRUE;
import static org.apache.curator.shaded.com.google.common.base.Preconditions.checkArgument;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AbstractAmqpQueue;
import io.streamnative.pulsar.handlers.amqp.AmqpEntryWriter;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpQueueProperties;
import io.streamnative.pulsar.handlers.amqp.ExchangeContainer;
import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.PulsarTopicMetadataUtils;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.WaitingEntryCallBack;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.jetbrains.annotations.NotNull;

/**
 * Persistent queue.
 */
@Slf4j
public class PersistentQueue extends AbstractAmqpQueue {
    public static final String QUEUE = "QUEUE";
    public static final String ROUTERS = "ROUTERS";
    public static final String TOPIC_PREFIX = "__amqp_queue__";
    public static final String DURABLE = "DURABLE";
    public static final String PASSIVE = "PASSIVE";
    public static final String EXCLUSIVE = "EXCLUSIVE";
    public static final String AUTO_DELETE = "AUTO_DELETE";
    public static final String INTERNAL = "INTERNAL";
    public static final String ARGUMENTS = "ARGUMENTS";
    public static final String X_DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange";
    public static final String X_MESSAGE_TTL = "x-message-ttl";
    public static final String X_DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
    public static final String DEFAULT_SUBSCRIPTION = "AMQP_DEFAULT";
    public static final long DELAY_1000 = 1000;
    public static final long MAX_TTL = 50L * 24 * 60 * 60 * 1000;

    @Getter
    private final PersistentTopic indexTopic;

    private final ObjectMapper jsonMapper;

    private AmqpEntryWriter amqpEntryWriter;

    private CompletableFuture<Producer<byte[]>> deadLetterProducer;
    private String deadLetterExchange;
    private long queueMessageTtl;
    private String deadLetterRoutingKey;
    private PersistentSubscription defaultSubscription;

    private final ScheduledExecutorService scheduledExecutor;

    private volatile int isActive = FALSE;

    private static final AtomicIntegerFieldUpdater<PersistentQueue> ACTIVE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentQueue.class, "isActive");

    private volatile int isWaiting = FALSE;

    private static final AtomicIntegerFieldUpdater<PersistentQueue> WAITING_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentQueue.class, "isWaiting");

    private volatile int retry = 0;

    private static final AtomicIntegerFieldUpdater<PersistentQueue> RETRY_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(PersistentQueue.class, "retry");

    public PersistentQueue(String queueName, PersistentTopic indexTopic,
                           long connectionId,
                           boolean exclusive, boolean autoDelete, Map<String, String> properties) {
        super(queueName, true, connectionId, exclusive, autoDelete, properties);
        this.indexTopic = indexTopic;
        this.scheduledExecutor = indexTopic.getBrokerService().executor();
        topicNameValidate();
        this.jsonMapper = new ObjectMapper();
        this.amqpEntryWriter = new AmqpEntryWriter(indexTopic);
    }

    private CompletableFuture<Void> initMessageExpire() {
        String args = properties.get(ARGUMENTS);
        if (StringUtils.isNotBlank(args)) {
            arguments.putAll(QueueUtil.covertStringValueAsObjectMap(args));
            this.deadLetterExchange = (String) arguments.get(X_DEAD_LETTER_EXCHANGE);
            Object messageTtl = arguments.get(X_MESSAGE_TTL);
            if (messageTtl != null && NumberUtils.isCreatable(messageTtl.toString())) {
                this.queueMessageTtl = NumberUtils.createLong(messageTtl.toString());
            }
            this.deadLetterRoutingKey = (String) arguments.get(X_DEAD_LETTER_ROUTING_KEY);

            if (StringUtils.isNotBlank(deadLetterExchange)) {
                // init producer
                if (StringUtils.isBlank(deadLetterRoutingKey)) {
                    this.deadLetterRoutingKey = "";
                }
                NamespaceName namespaceName = TopicName.get(indexTopic.getName()).getNamespaceObject();
                String topic = getTopicName(PersistentExchange.TOPIC_PREFIX,
                        namespaceName.getTenant(), namespaceName.getLocalName(), deadLetterExchange);
                if (indexTopic.getBrokerService().getPulsar().getProtocolHandlers()
                        .protocol("amqp") instanceof AmqpProtocolHandler protocolHandler) {
                    return protocolHandler.getAmqpBrokerService().getAmqpAdmin()
                            .loadExchange(namespaceName, deadLetterExchange)
                            .thenCompose(__ -> this.deadLetterProducer = initDeadLetterProducer(indexTopic, topic))
                            .thenApply(__-> null);
                }
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Void> startMessageExpireChecker() {
        if (ACTIVE_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            return initMessageExpire()
                    .thenCompose(__-> initDefaultSubscription())
                    .thenAcceptAsync(subscription -> {
                        RETRY_UPDATER.set(this, 0);
                        this.defaultSubscription = (PersistentSubscription) subscription;
                        // start check expired
                        readEntries();
                        log.info("[{}] Message expiration checker started successfully", indexTopic.getName());
                    }).exceptionally(throwable -> {
                        log.warn("Retry count:{} Queue {} DQL is not created, DQL: {} ,ex {}",
                                retry, queueName, deadLetterExchange, throwable);
                        ACTIVE_UPDATER.compareAndSet(this, TRUE, FALSE);
                        // TODO  Application alarm notification needs to be added
                        scheduledExecutor.schedule(this::startMessageExpireChecker, 10 * DELAY_1000, TimeUnit.MILLISECONDS);
                        return null;
                    });
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<Subscription> initDefaultSubscription() {
        Subscription subscription = indexTopic.getSubscriptions().get(DEFAULT_SUBSCRIPTION);
        if (subscription != null) {
            return CompletableFuture.completedFuture(subscription);
        }
        return indexTopic.createSubscription(DEFAULT_SUBSCRIPTION, CommandSubscribe.InitialPosition.Earliest, false,
                Collections.emptyMap());
    }

    class WaitingCallBack implements WaitingEntryCallBack {

        public WaitingCallBack() {
        }

        @Override
        public void entriesAvailable() {
            if (isActive == TRUE && WAITING_UPDATER.compareAndSet(PersistentQueue.this, TRUE, FALSE)) {
                PersistentQueue.this.scheduledExecutor.execute(PersistentQueue.this::readEntries);
            }
        }
    }

    private void readEntries() {
        if (isActive == FALSE) {
            return;
        }
        // 1. If there are active consumers, stop monitoring
        // 2. Start detection when the consumer is closed and enter the ledger's wait queue
        // 3. When the waiting queue is woken up, the detection again detects whether there is a consumer. If there is
        // no consumer, then read it.
        if (defaultSubscription.getDispatcher() != null && defaultSubscription.getDispatcher()
                .isConsumerConnected()) {
            log.warn("[{}] There are active consumers to stop monitoring", queueName);
            ACTIVE_UPDATER.set(this, FALSE);
            return;
        }
        ManagedCursor cursor = defaultSubscription.getCursor();
        if (defaultSubscription.getNumberOfEntriesInBacklog(false) == 0) {
            if (cursor.getManagedLedger() instanceof ManagedLedgerImpl managedLedger) {
                if (managedLedger.isTerminated()) {
                    log.warn("[{}]ledger is close", queueName);
                    ACTIVE_UPDATER.set(this, FALSE);
                    return;
                }
                log.warn("[{}]start waiting read.", queueName);
                if (WAITING_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                    managedLedger.addWaitingEntryCallBack(new WaitingCallBack());
                }
            } else {
                scheduledExecutor.schedule(PersistentQueue.this::readEntries, 5 * DELAY_1000, TimeUnit.MILLISECONDS);
            }
            return;
        }
        // To send a message to the topic, the cursor is not read from the first, need to reset
        cursor.rewind();
        // Use cursor.asyncReadEntriesOrWait() can register only one wait.The user's consumer startup will fail.
        cursor.asyncReadEntries(1, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                if (entries.size() == 0) {
                    log.warn("[{}] read entries is 0, need retry", queueName);
                    scheduledExecutor.execute(PersistentQueue.this::readEntries);
                    return;
                }
                Entry entry = entries.get(0);
                ByteBuf dataBuffer = entry.getDataBuffer();
                Position expirePosition = entry.getPosition();
                MessageMetadata messageMetadata = Commands.parseMessageMetadata(dataBuffer);
                try {
                    // queue ttl
                    long expireTime = queueMessageTtl;
                    // message ttl
                    KeyValue keyValue = messageMetadata.getPropertiesList().stream()
                            .filter(kv -> MessageConvertUtils.PROP_EXPIRATION.equals(kv.getKey()))
                            .findFirst()
                            .orElse(null);
                    long messageTtl;
                    if (keyValue != null && (messageTtl = Long.parseLong(keyValue.getValue())) > 0) {
                        expireTime = expireTime == 0 ? messageTtl : Math.min(expireTime, messageTtl);
                    }
                    // In most cases, the TTL is not available and the check task needs to be stopped.
                    if (expireTime == 0) {
                        // It is possible to mix expired messages with non-expired messages
                        // Stop check
                        // Sending a non-TTL message requires the presence of a consumer message. When a consumer
                        // exists, the current task will not be executed here.
                        ACTIVE_UPDATER.set(PersistentQueue.this, FALSE);
                        log.warn("[{}] Queue message TTL is not set, stop check trace", queueName);
                        return;
                    }
                    long expireMillis;
                    // no expire
                    if ((expireMillis = entryExpired(expireTime, messageMetadata.getPublishTime())) > 0) {
                        if (expireTime >= MAX_TTL) {
                            log.warn("[{}]There is a message with a very long expiration time {}.",
                                    defaultSubscription.getTopic().getName(), expireTime);
                            try {
                                indexTopic.getBrokerService()
                                        .pulsar()
                                        .getAdminClient()
                                        .topics()
                                        .setMessageTTL(defaultSubscription.getTopic().getName(),
                                                (int) (expireTime / 1000 + 24 * 60 * 60));
                            } catch (Exception e) {
                                log.error("[{}] Failed to reset topic ttl:{}.",
                                        defaultSubscription.getTopic().getName(), expireTime);
                                // TODO  Application alarm notification needs to be added
                            }
                        }
                        // The current topic may have been unloaded, isActive=false, when the task is awakened, filter
                        // the task through the isActive check
                        scheduledExecutor.schedule(PersistentQueue.this::readEntries, expireMillis,
                                TimeUnit.MILLISECONDS);
                        return;
                    }
                    // expire but no dead letter queue
                    if (deadLetterProducer == null) {
                        log.warn("Message expired, no dead-letter-producer, [{}] message auto ack [{}]", queueName,
                                expirePosition);
                        // ack
                        makeAck(expirePosition, cursor).thenRun(
                                        () -> scheduledExecutor.execute(PersistentQueue.this::readEntries))
                                .exceptionally(throwable -> {
                                    log.error("no dead-letter-producer ack fail", throwable);
                                    scheduledExecutor.schedule(PersistentQueue.this::readEntries, 5 * DELAY_1000,
                                            TimeUnit.MILLISECONDS);
                                    return null;
                                });
                        return;
                    }
                    dataBuffer.retain();
                } finally {
                    entry.release();
                }
                messageMetadata.clearSequenceId();
                messageMetadata.clearPublishTime();
                messageMetadata.clearProducerName();
                messageMetadata.getPropertiesList().forEach(kv -> {
                    switch (kv.getKey()) {
                        case MessageConvertUtils.PROP_ROUTING_KEY -> kv.setValue(deadLetterRoutingKey);
                        case MessageConvertUtils.PROP_EXPIRATION -> kv.setValue("0");
                        case MessageConvertUtils.PROP_EXCHANGE -> kv.setValue(deadLetterExchange);
                        default -> {
                        }
                    }
                });
                final int readerIndex = dataBuffer.readerIndex();
                dataBuffer.readerIndex(readerIndex);
                deadLetterProducer.thenCompose(producer -> {
                            MessageImpl<byte[]> message = MessageImpl.create(null, null, messageMetadata,
                                    dataBuffer, Optional.empty(), null, Schema.BYTES,
                                    0, false, -1L);
                            return ((ProducerImpl<byte[]>) producer).sendAsync(message);
                        })
                        .thenAccept(__ -> makeAck(expirePosition, cursor))
                        .thenRun(() -> {
                            // Read the next one immediately
                            scheduledExecutor.execute(PersistentQueue.this::readEntries);
                        })
                        .whenComplete((__, throwable) -> {
                            ReferenceCountUtil.safeRelease(dataBuffer);
                            if (throwable != null) {
                                log.error("[{}] Failed to send a dead letter queue", queueName, throwable);
                                scheduledExecutor.schedule(PersistentQueue.this::readEntries, 5 * DELAY_1000,
                                        TimeUnit.MILLISECONDS);
                            }
                        });
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                log.error("[{}]Failed to read entries", queueName, exception);
                scheduledExecutor.schedule(PersistentQueue.this::readEntries, 5 * DELAY_1000, TimeUnit.MILLISECONDS);
            }
        }, null, null);
    }

    @NotNull
    private CompletableFuture<Void> makeAck(Position position, ManagedCursor cursor) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        cursor.asyncDelete(position, new AsyncCallbacks.DeleteCallback() {
            @Override
            public void deleteComplete(Object ctx) {
                future.complete(null);
            }

            @Override
            public void deleteFailed(ManagedLedgerException exception,
                                     Object ctx) {
                log.error("[{}] Message expired to delete exception, position {}", queueName,
                        position, exception);
                future.completeExceptionally(exception);
            }
        }, position);
        return future;
    }

    public static long entryExpired(long expireMillis, long entryTimestamp) {
        return (entryTimestamp + expireMillis) - System.currentTimeMillis();
    }

    private CompletableFuture<Producer<byte[]>> initDeadLetterProducer(PersistentTopic indexTopic, String topic) {
        try {
            return indexTopic.getBrokerService().pulsar()
                    .getClient()
                    .newProducer()
                    .topic(topic)
                    .enableBatching(false)
                    .createAsync();
        } catch (Exception e) {
            log.error("init dead letter producer fail", e);
            throw new AoPServiceRuntimeException.ProducerCreationRuntimeException(e);
        }
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
        return getRouter(exchangeName).getExchange().markDeleteAsync(getName(), ledgerId, entryId);
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

    @Override
    public void close() {
        ACTIVE_UPDATER.set(this, FALSE);
        if (deadLetterProducer != null) {
            deadLetterProducer.thenAccept(Producer::closeAsync);
        }
    }

}
