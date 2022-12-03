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

import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.FALSE;
import static org.apache.bookkeeper.mledger.impl.ManagedCursorImpl.TRUE;
import static org.apache.pulsar.broker.service.persistent.PersistentTopic.MESSAGE_RATE_BACKOFF_MS;

import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
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

    private ManagedCursorImpl cursor;

    private final Map<String, CompletableFuture<Producer<byte[]>>> producerMap = new ConcurrentHashMap<>();

    private static final int defaultReadMaxSizeBytes = 4 * 1024 * 1024;
    private static final int replicatorQueueSize = 1000;
    private volatile int pendingQueueSize = 0;

    private static final AtomicIntegerFieldUpdater<ExchangeMessageRouter> PENDING_SIZE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ExchangeMessageRouter.class, "pendingQueueSize");

    private volatile int havePendingRead = FALSE;
    private static final AtomicIntegerFieldUpdater<ExchangeMessageRouter> HAVE_PENDING_READ_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(ExchangeMessageRouter.class, "havePendingRead");

    @AllArgsConstructor
    @EqualsAndHashCode
    private static class Destination {
        String name;
        String type;
    }

    public ExchangeMessageRouter(PersistentExchange exchange) {
        this.exchange = exchange;
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
                        readMoreEntries();
                    }

                    @Override
                    public void openCursorFailed(ManagedLedgerException exception, Object ctx) {
                        log.error("Failed to open cursor for exchange topic {}, retry", exchange.getName(), exception);
                        start0(managedLedger);
                    }
        }, null);
    }

    private void readMoreEntries() {
        int availablePermits = getAvailablePermits();
        if (availablePermits > 0) {
            if (HAVE_PENDING_READ_UPDATER.compareAndSet(this, FALSE, TRUE)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} Schedule read of {} messages.", exchange.getName(), availablePermits);
                }
                cursor.asyncReadEntriesOrWait(100, defaultReadMaxSizeBytes,
                        new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        HAVE_PENDING_READ_UPDATER.set(ExchangeMessageRouter.this, FALSE);
                        processMessages(entries);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        HAVE_PENDING_READ_UPDATER.set(ExchangeMessageRouter.this, FALSE);
                        log.error("Failed to read entries from exchange {}", exchange.getName(), exception);
                    }
                }, null, null);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{} Not schedule read due to pending read. Messages to read {}.",
                            exchange.getName(), availablePermits);
                }
            }
        } else {
            // no permits from rate limit
            exchange.getTopic().getBrokerService().getPulsar().getExecutor()
                    .schedule(this::readMoreEntries, MESSAGE_RATE_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

    private int getAvailablePermits() {
        int availablePermits = replicatorQueueSize - PENDING_SIZE_UPDATER.get(this);
        if (availablePermits <= 0) {
            if (log.isDebugEnabled()) {
                log.debug("{} Replicator queue is full, availablePermits: {}, pause route.",
                        exchange.getName(), availablePermits);
            }
            return 0;
        }
        return availablePermits;
    }

    private void processMessages(List<Entry> entries) {
        for (Entry entry : entries) {
            PENDING_SIZE_UPDATER.incrementAndGet(this);
            Map<String, String> props;
            MessageImpl<byte[]> message;
            try {
                message = MessageImpl.create(null, null,
                        Commands.parseMessageMetadata(entry.getDataBuffer()),
                        entry.getDataBuffer(),
                        null, null, Schema.BYTES, 0, true, -1L);
//                message = MessageImpl.deserialize(entry.getDataBuffer());
                props = message.getMessageBuilder().getPropertiesList().stream()
                        .collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
            } catch (Exception e) {
                log.error("Deserialize entry dataBuffer failed for exchange {}, skip it first.",
                        exchange.getName(), e);
                PENDING_SIZE_UPDATER.decrementAndGet(this);
                entry.release();
                continue;
            }
            String routingKey = props.getOrDefault(MessageConvertUtils.PROP_ROUTING_KEY, "").toString();
            Set<Destination> destinations = getDestinations(routingKey, getMessageHeaders());

            final Position position = entry.getPosition();

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            if (!destinations.isEmpty()) {
                entry.getDataBuffer().retain(destinations.size());
                final int readerIndex = message.getDataBuffer().readerIndex();
                for (Destination des : destinations) {
                    futures.add(sendMessage(message, des, readerIndex));
                }
            }
            entry.release();
            FutureUtil.waitForAll(futures).whenComplete((__, t) -> {
                if (t != null) {
                    log.error("Failed to route message {}", position, t);
                    cursor.rewind();
                    sendComplete();
                    return;
                }
                sendComplete();
                cursor.asyncDelete(position, new AsyncCallbacks.DeleteCallback() {
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
                }, entry.getPosition());
            });
        }
    }

    private void sendComplete() {
        int pending = PENDING_SIZE_UPDATER.decrementAndGet(this);
        if (pending == 0 && HAVE_PENDING_READ_UPDATER.get(this) == FALSE) {
            this.readMoreEntries();
        }
    }

    private CompletableFuture<Void> sendMessage(MessageImpl<byte[]> msg, Destination des, int readerIndex) {
        return getProducer(des.name, des.type)
                .thenCompose(producer -> {
                    msg.getMessageBuilder().clearProducerName();
                    msg.getMessageBuilder().clearPublishTime();
                    msg.getDataBuffer().readerIndex(readerIndex);
                    return ((ProducerImpl<byte[]>) producer).sendAsync(msg);
                })
                .thenApply(__ -> null);
    }

    private CompletableFuture<Producer<byte[]>> getProducer(String des, String desType) {
        PulsarClient pulsarClient;
        try {
            pulsarClient = exchange.getTopic().getBrokerService().pulsar().getClient();
        } catch (PulsarServerException e) {
            log.error("Failed to get pulsar client", e);
            return FutureUtil.failedFuture(e);
        }
        NamespaceName namespaceName = TopicName.get(exchange.getTopic().getName()).getNamespaceObject();
        String prefix = desType.equals("queue") ? PersistentQueue.TOPIC_PREFIX : PersistentExchange.TOPIC_PREFIX;
        return producerMap.computeIfAbsent(des, k -> pulsarClient.newProducer()
                .topic(TopicName.get(TopicDomain.persistent.toString(), namespaceName, prefix + des).toString())
                .enableBatching(false)
                .createAsync());
    }

    protected Map<String, Object> getMessageHeaders() {
        return null;
    }

    public static ExchangeMessageRouter getInstance(PersistentExchange exchange) {
        return switch (exchange.getType()) {
            case Fanout -> new FanoutExchangeMessageRouter(exchange);
            case Direct -> new DirectExchangeMessageRouter(exchange);
            case Topic -> new TopicExchangeMessageRouter(exchange);
            case Headers -> new HeadersExchangeMessageRouter(exchange);
        };
    }

    static class FanoutExchangeMessageRouter extends ExchangeMessageRouter {

        private final Set<Destination> destinationSet;

        public FanoutExchangeMessageRouter(PersistentExchange exchange) {
            super(exchange);
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

        public DirectExchangeMessageRouter(PersistentExchange exchange) {
            super(exchange);
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

        public TopicExchangeMessageRouter(PersistentExchange exchange) {
            super(exchange);
            destinationMap = new ConcurrentHashMap<>();
        }

        static class TopicRoutingKeyParser {

            Set<String> bindingKeys;
            TopicParser topicParser;

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

        public HeadersExchangeMessageRouter(PersistentExchange exchange) {
            super(exchange);
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
            messageRouterMap.computeIfPresent(new Destination(des, desType), (k, v) -> {
                v.getArguments().putAll(arguments);
                if (v.getArguments().isEmpty()) {
                    return null;
                }
                return v;
            });
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

}
