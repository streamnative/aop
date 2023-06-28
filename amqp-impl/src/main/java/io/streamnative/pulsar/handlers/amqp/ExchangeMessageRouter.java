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
package io.streamnative.pulsar.handlers.amqp;

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.FALSE;
import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.TRUE;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.exchange.topic.TopicParser;

@Slf4j
public abstract class ExchangeMessageRouter {

    private final PersistentExchange exchange;
    private final ExecutorService routeExecutor;

    private ManagedCursorImpl cursor;

    private final Map<String, ProducerImpl<byte[]>> producerMap = new ConcurrentHashMap<>();

    private static final int defaultReadMaxSizeBytes = 5 * 1024 * 1024;
    private static final int replicatorQueueSize = 2000;
    private volatile int pendingQueueSize = 0;

    private static final AtomicIntegerFieldUpdater<ExchangeMessageRouter> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ExchangeMessageRouter.class, "pendingQueueSize");

    private volatile int havePendingRead = FALSE;
    private static final AtomicIntegerFieldUpdater<ExchangeMessageRouter> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ExchangeMessageRouter.class, "havePendingRead");
    private volatile int isActive = FALSE;
    private static final AtomicIntegerFieldUpdater<ExchangeMessageRouter> ACTIVE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ExchangeMessageRouter.class, "isActive");
    @AllArgsConstructor
    @EqualsAndHashCode
    private static class Destination {
        String name;
        String type;
    }

    public ExchangeMessageRouter(PersistentExchange exchange, ExecutorService routeExecutor) {
        this.exchange = exchange;
        this.routeExecutor = routeExecutor;
    }

    public abstract void addBinding(String des, String desType, String routingKey, Map<String, Object> arguments);

    public abstract void removeBinding(String des, String desType, String routingKey, Map<String, Object> arguments);

    abstract Set<Destination> getDestinations(String routingKey, Map<String, Object> headers);

    public void start() {
        start0((ManagedLedgerImpl) ((PersistentTopic) exchange.getTopic()).getManagedLedger());
    }

    private void start0(ManagedLedgerImpl managedLedger) {
        managedLedger.asyncOpenCursor("amqp-router", CommandSubscribe.InitialPosition.Earliest,
                new AsyncCallbacks.OpenCursorCallback() {
                    @Override
                    public void openCursorComplete(ManagedCursor cursor, Object ctx) {
                        log.info("Start to route messages for exchange {}", exchange.getName());
                        ExchangeMessageRouter.this.cursor = (ManagedCursorImpl) cursor;
                        if (ACTIVE_UPDATER.compareAndSet(ExchangeMessageRouter.this, FALSE, TRUE)) {
                            readMoreEntries();
                        }
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to open cursor for exchange topic {}, retry", exchange.getName(), exception);
                        start0(managedLedger);
                    }
                }, null);
    }

    private void readMoreEntries() {
        if (isActive == FALSE) {
            return;
        }
        if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
            cursor.asyncReadEntriesOrWait(replicatorQueueSize, defaultReadMaxSizeBytes,
                    new AsyncCallbacks.ReadEntriesCallback() {
                        @Override
                        public void readEntriesComplete(List<Entry> entries, Object ctx) {
                            if (entries.size() == 0) {
                                HAVE_PENDING_READ_UPDATER.set(ExchangeMessageRouter.this, FALSE);
                                log.warn("read empty entries, scheduled to read again.");
                                exchange.getTopic().getBrokerService().getPulsar().getExecutor()
                                        .schedule(ExchangeMessageRouter.this::readMoreEntries, 1,
                                                TimeUnit.MILLISECONDS);
                                return;
                            }
                            routeExecutor.submit(() -> {
                                try {
                                    routeMessages(entries);
                                    entries.clear();
                                } catch (Exception e) {
                                    log.error("Failed to route messages.", e);
                                    if (e instanceof AoPServiceRuntimeException.ProducerCreationRuntimeException) {
                                        producerMap.values().forEach(ProducerImpl::closeAsync);
                                        producerMap.clear();
                                    }
                                    cursor.rewind();
                                    for (Entry entry : entries) {
                                        ReferenceCountUtil.safeRelease(entry);
                                    }
                                }
                                HAVE_PENDING_READ_UPDATER.set(ExchangeMessageRouter.this, FALSE);
                                ExchangeMessageRouter.this.readMoreEntries();
                            });
                        }

                        @Override
                        public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                            HAVE_PENDING_READ_UPDATER.set(ExchangeMessageRouter.this, FALSE);
                            log.error("Failed to read entries from exchange {}", exchange.getName(), exception);
                            exchange.getTopic().getBrokerService().getPulsar().getExecutor()
                                    .schedule(() -> {
                                        cursor.rewind();
                                        readMoreEntries();
                                    }, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
                        }
                    }, null, null);
        } else {
            log.warn("{} Not schedule read due to pending read.",
                    exchange.getName());
        }
    }

    private int getAvailablePermits() {
        int availablePermits = replicatorQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            log.warn("{} Replicator queue is full, availablePermits: {}, pause route.",
                    exchange.getName(), availablePermits);
            if (log.isDebugEnabled()) {
                log.debug("{} Replicator queue is full, availablePermits: {}, pause route.",
                        exchange.getName(), availablePermits);
            }
            return 0;
        }
        return availablePermits;
    }

    private void routeMessages(List<Entry> entries) {
        List<Position> positions = new ArrayList<>(entries.size());
        try {
            for (Entry entry : entries) {
                final Position position = entry.getPosition();
                ByteBuf dataBuffer = entry.getDataBuffer();
                Map<String, String> props;
                MessageImpl<byte[]> message;
                try {
                    message = MessageImpl.create(null, null, Commands.parseMessageMetadata(dataBuffer), dataBuffer,
                            Optional.empty(), null, Schema.BYTES, 0, true, -1L);
                    props = message.getMessageBuilder().getPropertiesList().stream()
                            .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                } catch (Exception e) {
                    log.error("Deserialize entry dataBuffer failed for exchange {}, skip it first.", exchange.getName(), e);
                    entry.release();
                    dataBuffer.release();
                    continue;
                }

                Set<Destination> destinations = getDestinations(
                        props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, ""), getMessageHeaders());
                boolean notNull = destinations != null;
                List<CompletableFuture<MessageId>> futures = notNull ?
                        new ArrayList<>(destinations.size()) : Collections.emptyList();
                try {
                    if (notNull && !destinations.isEmpty()) {
                        initProducerIfNeeded(destinations);
                        String xDelay;
                        int delay;
                        if (exchange.isExistDelayedType()
                                && StringUtils.isNotBlank(xDelay = props.get(MessageConvertUtils.BASIC_PROP_HEADER_X_DELAY))
                                && NumberUtils.isNumber(xDelay)
                                && (delay = Integer.parseInt(xDelay)) > 0) {
                            message.getMessageBuilder().setDeliverAtTime(System.currentTimeMillis() + delay);
                        }
                        if (destinations.size() > 1) {
                            dataBuffer.retain(destinations.size() - 1);
                        }
                        final int readerIndex = dataBuffer.readerIndex();
                        for (Destination des : destinations) {
                            ProducerImpl<byte[]> producer = producerMap.get(des.name);
                            message.getMessageBuilder().clearSequenceId();
                            message.getMessageBuilder().clearProducerName();
                            message.getMessageBuilder().clearPublishTime();
                            dataBuffer.readerIndex(readerIndex);
                            futures.add(producer.sendAsync(message));
                        }
                    } else {
                        dataBuffer.release();
                    }
                } finally {
                    entry.release();
                }
                // If the producer creates an exception, add is not executed
                positions.add(position);
                FutureUtil.waitForAll(futures).exceptionally((t) -> {
                    if (t != null) {
                        log.error("Failed to route message {} for exchange {}.", position, exchange.exchangeName, t);
                        // TODO  Application alarm notification needs to be added
                    }
                    return null;
                });
            }
        } finally {
            if (positions.size() != 0) {
                cursor.asyncDelete(positions, new AsyncCallbacks.DeleteCallback() {
                    @Override
                    public void deleteComplete(Object ctx) {
                        if (log.isDebugEnabled()) {
                            log.debug("{} Deleted message at {}", exchange.getName(), ctx);
                        }
                    }

                    @Override
                    public void deleteFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("{} Failed to delete message at {}", exchange.getName(), ctx, exception);
                    }
                }, null);
                positions.clear();
            }
        }
    }

    private void tryToReadMoreEntries() {
        if (PENDING_SIZE_UPDATER.decrementAndGet(this) < replicatorQueueSize * 0.5
                && HAVE_PENDING_READ_UPDATER.get(this) == FALSE) {
            this.readMoreEntries();
        }
    }

    private void initProducerIfNeeded(Set<Destination> destinations) {
        PulsarClient pulsarClient = exchange.getPulsarClient();
        for (Destination des : destinations) {
            producerMap.computeIfAbsent(des.name, k -> {
                String topic = getTopic(des.name, des.type);
                try {
                    return (ProducerImpl<byte[]>) pulsarClient.newProducer()
                            .topic(topic)
                            .enableBatching(false)
                            .blockIfQueueFull(true)
                            .maxPendingMessages(20000)
                            .sendTimeout(0, TimeUnit.MILLISECONDS)
                            .create();
                } catch (PulsarClientException e) {
                    throw new AoPServiceRuntimeException.ProducerCreationRuntimeException(e);
                }
            });
        }
    }

    private String getTopic(String des, String desType) {
        NamespaceName namespaceName = TopicName.get(exchange.getTopic().getName()).getNamespaceObject();
        String prefix = desType.equals("queue") ? PersistentQueue.TOPIC_PREFIX : PersistentExchange.TOPIC_PREFIX;
        return TopicName.get(TopicDomain.persistent.toString(), namespaceName, prefix + des).toString();
    }

    protected Map<String, Object> getMessageHeaders() {
        return null;
    }

    public static ExchangeMessageRouter getInstance(PersistentExchange exchange, ExecutorService routeExecutor) {
        return switch (exchange.getType()) {
            case Fanout -> new FanoutExchangeMessageRouter(exchange, routeExecutor);
            case Direct -> new DirectExchangeMessageRouter(exchange, routeExecutor);
            case Topic -> new TopicExchangeMessageRouter(exchange, routeExecutor);
            case Headers -> new HeadersExchangeMessageRouter(exchange, routeExecutor);
            default -> throw new AoPServiceRuntimeException.NotSupportedExchangeTypeException(
                    exchange.getType() + " not support");
        };
    }

    static class FanoutExchangeMessageRouter extends ExchangeMessageRouter {

        private final Set<Destination> destinationSet;

        public FanoutExchangeMessageRouter(PersistentExchange exchange, ExecutorService routeExecutor) {
            super(exchange, routeExecutor);
            destinationSet = Sets.newConcurrentHashSet();
        }

        @Override
        public synchronized void addBinding(String des, String desType, String routingKey,
                                            Map<String, Object> arguments) {
            destinationSet.add(new Destination(des, desType));
        }

        @Override
        public synchronized void removeBinding(String des, String desType, String routingKey,
                                               Map<String, Object> arguments) {
            destinationSet.remove(new Destination(des, desType));
        }

        @Override
        Set<Destination> getDestinations(String routingKey, Map<String, Object> headers) {
            return destinationSet;
        }

    }

    static class DirectExchangeMessageRouter extends ExchangeMessageRouter {

        private final Map<String, Set<Destination>> destinationMap;

        public DirectExchangeMessageRouter(PersistentExchange exchange, ExecutorService routeExecutor) {
            super(exchange, routeExecutor);
            destinationMap = new ConcurrentHashMap<>();
        }

        @Override
        public synchronized void addBinding(String des, String desType, String routingKey,
                                            Map<String, Object> arguments) {
            destinationMap.computeIfAbsent(routingKey, k -> Sets.newConcurrentHashSet())
                    .add(new Destination(des, desType));
        }

        @Override
        public synchronized void removeBinding(String des, String desType, String routingKey,
                                               Map<String, Object> arguments) {
            destinationMap.computeIfPresent(routingKey, (k, v) -> {
                v.remove(new Destination(des, desType));
                if (v.isEmpty()) {
                    return null;
                }
                return v;
            });
        }

        @Override
        Set<Destination> getDestinations(String routingKey, Map<String, Object> headers) {
            return destinationMap.get(routingKey);
        }
    }

    static class TopicExchangeMessageRouter extends ExchangeMessageRouter {

        private final Map<Destination, TopicRoutingKeyParser> destinationMap;

        public TopicExchangeMessageRouter(PersistentExchange exchange, ExecutorService routeExecutor) {
            super(exchange, routeExecutor);
            destinationMap = new ConcurrentHashMap<>();
        }

        static class TopicRoutingKeyParser {

            final Set<String> bindingKeys;
            TopicParser topicParser;

            TopicRoutingKeyParser() {
                this.bindingKeys = new HashSet<>();
            }

            void addBinding(String routingKey) {
                if (bindingKeys.add(routingKey)) {
                    topicParser = new TopicParser();
                    topicParser.addBinding(routingKey, null);
                }
            }

            void unbind(String routingKey) {
                bindingKeys.remove(routingKey);
                topicParser = new TopicParser();
                for (String bindingKey : bindingKeys) {
                    topicParser.addBinding(bindingKey, null);
                }
            }

        }

        @Override
        public synchronized void addBinding(String des, String desType, String routingKey,
                                            Map<String, Object> arguments) {
            destinationMap.computeIfAbsent(new Destination(des, desType), k -> new TopicRoutingKeyParser())
                    .addBinding(routingKey);
        }

        @Override
        public synchronized void removeBinding(String des, String desType, String routingKey,
                                               Map<String, Object> arguments) {
            destinationMap.computeIfPresent(new Destination(des, desType), (k, v) -> {
                v.unbind(routingKey);
                if (v.bindingKeys.isEmpty()) {
                    return null;
                }
                return v;
            });
        }

        @Override
        Set<Destination> getDestinations(String routingKey, Map<String, Object> headers) {
            Set<Destination> destinations = new HashSet<>();
            for (Map.Entry<Destination, TopicRoutingKeyParser> entry : destinationMap.entrySet()) {
                if (!entry.getValue().topicParser.parse(routingKey).isEmpty()) {
                    destinations.add(entry.getKey());
                }
            }
            return destinations;
        }
    }

    static class HeadersExchangeMessageRouter extends ExchangeMessageRouter {

        private final Map<Destination, HeadersMessageRouter> messageRouterMap;

        public HeadersExchangeMessageRouter(PersistentExchange exchange, ExecutorService routeExecutor) {
            super(exchange, routeExecutor);
            messageRouterMap = new ConcurrentHashMap<>();
        }

        @Override
        public synchronized void addBinding(String des, String desType, String routingKey,
                                            Map<String, Object> arguments) {
            messageRouterMap.computeIfAbsent(new Destination(des, desType), k -> new HeadersMessageRouter())
                    .getArguments().putAll(arguments);
        }

        @Override
        public synchronized void removeBinding(String des, String desType, String routingKey,
                                               Map<String, Object> arguments) {
            messageRouterMap.remove(new Destination(des, desType));
        }

        @Override
        Set<Destination> getDestinations(String routingKey, Map<String, Object> headers) {
            Set<Destination> destinations = new HashSet<>();
            for (Map.Entry<Destination, HeadersMessageRouter> entry : messageRouterMap.entrySet()) {
                if (entry.getValue().isMatch(headers)) {
                    destinations.add(entry.getKey());
                }
            }
            return destinations;
        }

    }

    public void close() {
        ACTIVE_UPDATER.set(this, FALSE);
        producerMap.values().forEach(ProducerImpl::closeAsync);
        producerMap.clear();
    }
}
