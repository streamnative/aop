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
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.JSON_MAPPER;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.admin.model.MessageBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.MessageParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.PurgeQueueParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueueBinds;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueueDetail;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueuesList;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.PrometheusAdmin;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.QueueListMetrics;
import io.streamnative.pulsar.handlers.amqp.admin.prometheus.QueueRangeMetrics;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import io.streamnative.pulsar.handlers.amqp.utils.TopicUtil;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.jetbrains.annotations.NotNull;

/**
 * QueueBase.
 */
@Slf4j
public class QueueBase extends BaseResources {

    protected CompletableFuture<List<QueueBean>> getQueueListAsync() {
        final List<QueueBean> list = new ArrayList<>();
        return namespaceResource().listNamespacesAsync(tenant)
                .thenCompose(nsList -> {
                    Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
                    for (String ns : nsList) {
                        futureList.add(getQueueListByVhostAsync(ns).thenAccept(list::addAll));
                    }
                    return FutureUtil.waitForAll(futureList);
                }).thenApply(__ -> list);
    }

    @Override
    protected CompletableFuture<List<VhostBean>> getVhostListAsync() {
        return namespaceResource().listNamespacesAsync(tenant)
                .thenApply(nsList -> {
                    List<VhostBean> vhostBeanList = new ArrayList<>();
                    nsList.forEach(ns -> {
                        VhostBean bean = new VhostBean();
                        bean.setName(ns);
                        vhostBeanList.add(bean);
                    });
                    return vhostBeanList;
                });
    }

    protected CompletableFuture<List<String>> getQueueListAsync(String tenant, String ns) {
        return namespaceService()
                .getFullListOfTopics(NamespaceName.get(tenant, ns))
                .thenApply(list -> list.stream().filter(s ->
                        s.contains(PersistentQueue.TOPIC_PREFIX)).collect(Collectors.toList()));
    }

    protected CompletableFuture<List<QueuesList.ItemsBean>> getQueueListByNamespaceAsync(String vhost, String sort,
                                                                                         boolean sortReverse) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        return getQueueListAsync(namespaceName.getTenant(), namespaceName.getLocalName())
                .thenCompose(exList -> {
                    Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
                    List<QueuesList.ItemsBean> beanList = new CopyOnWriteArrayList<>();
                    exList.forEach(topic -> {
                        String queue = TopicName.get(topic).getLocalName()
                                .substring(PersistentQueue.TOPIC_PREFIX.length());
                        futureList.add(getQueueBeanByNamespaceAsync(namespaceName, queue)
                                .thenAccept(itemsBean -> {
                                    if (itemsBean != null) {
                                        beanList.add(itemsBean);
                                    }
                                }));
                    });
                    return FutureUtil.waitForAll(futureList).thenApply(__ -> beanList);
                }).thenCompose(itemsBeans -> {
                    PrometheusAdmin prometheusAdmin = prometheusAdmin();
                    if (!prometheusAdmin.isConfig()) {
                        return CompletableFuture.completedFuture(itemsBeans);
                    }
                    return prometheusAdmin.queryQueueListMetrics(
                                    namespaceName.getTenant() + "/" + namespaceName.getLocalName())
                            .thenApply(queueListMetricsMap -> {
                                itemsBeans.forEach(itemsBean -> {
                                    QueueListMetrics metrics =
                                            queueListMetricsMap.get(itemsBean.getFullName());
                                    if (metrics == null) {
                                        return;
                                    }
                                    itemsBean.setMessages_ready(metrics.getReady());
                                    itemsBean.setMessages_unacknowledged(metrics.getUnAck());
                                    itemsBean.setMessages(metrics.getTotal());
                                    itemsBean.getMessage_stats().getAck_details().setRate(metrics.getAckRate());
                                    itemsBean.getMessage_stats().getPublish_details()
                                            .setRate(metrics.getIncomingRate());
                                    itemsBean.getMessage_stats().getDeliver_get_details()
                                            .setRate(metrics.getDeliverRate());
                                });
                                return itemsBeans;
                            }).thenApply(ib -> {
                                if (sort == null) {
                                    ib.sort(Comparator.comparing(QueuesList.ItemsBean::getName,
                                            Comparator.naturalOrder()));
                                    return ib;
                                }
                                switch (sort) {
                                    case "messages_ready" ->
                                            ib.sort(Comparator.comparing(QueuesList.ItemsBean::getMessages_ready,
                                                    sortReverse ? Comparator.reverseOrder() :
                                                            Comparator.naturalOrder()));
                                    case "messages_unacknowledged" -> ib.sort(Comparator.comparing(
                                            QueuesList.ItemsBean::getMessages_unacknowledged,
                                            sortReverse ? Comparator.reverseOrder() : Comparator.naturalOrder()));
                                    case "messages" -> ib.sort(Comparator.comparing(QueuesList.ItemsBean::getMessages,
                                            sortReverse ? Comparator.reverseOrder() : Comparator.naturalOrder()));
                                    case "message_stats.publish_details.rate" -> ib.sort(Comparator.comparing(
                                            (Function<QueuesList.ItemsBean, Double>) itemsBean -> itemsBean.getMessage_stats()
                                                    .getPublish_details().getRate(),
                                            sortReverse ? Comparator.reverseOrder() : Comparator.naturalOrder()));
                                    case "message_stats.deliver_get_details.rate" -> ib.sort(Comparator.comparing(
                                            (Function<QueuesList.ItemsBean, Double>) itemsBean -> itemsBean.getMessage_stats()
                                                    .getDeliver_get_details().getRate(),
                                            sortReverse ? Comparator.reverseOrder() : Comparator.naturalOrder()));
                                    case "message_stats.ack_details.rate" -> ib.sort(Comparator.comparing(
                                            (Function<QueuesList.ItemsBean, Double>) itemsBean -> itemsBean.getMessage_stats()
                                                    .getAck_details().getRate(),
                                            sortReverse ? Comparator.reverseOrder() : Comparator.naturalOrder()));
                                    default -> ib.sort(Comparator.comparing(QueuesList.ItemsBean::getName,
                                            Comparator.naturalOrder()));
                                }
                                return ib;
                            });
                });
    }

    protected CompletableFuture<List<QueueBean>> getQueueListByVhostAsync(String vhost) {
        return getQueueListAsync(tenant, vhost).thenCompose(exList -> {
            Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
            List<QueueBean> beanList = new ArrayList<>();
            exList.forEach(topic -> {
                String queue = TopicName.get(topic).getLocalName()
                        .substring(PersistentQueue.TOPIC_PREFIX.length());
                futureList.add(getQueueBeanAsync(vhost, queue).thenAccept(beanList::add));
            });
            return FutureUtil.waitForAll(futureList).thenApply(__ -> beanList);
        });
    }

    protected CompletableFuture<QueuesList.ItemsBean> getQueueBeanByNamespaceAsync(NamespaceName namespaceName,
                                                                                   String queue) {
        return getTopicProperties(namespaceName.toString(), PersistentQueue.TOPIC_PREFIX, queue).thenApply(
                properties -> {
                    if (properties == null || properties.isEmpty()) {
                        log.error("queue properties is null. topic:{}/{}", namespaceName, queue);
                        return null;
                    }
                    QueueDeclareParams declareParams = QueueUtil.covertMapAsParams(properties);
                    QueuesList.ItemsBean itemsBean = new QueuesList.ItemsBean();
                    itemsBean.setName(queue);
                    itemsBean.setFullName(TopicName.get(TopicDomain.persistent.value(), namespaceName,
                            PersistentQueue.TOPIC_PREFIX + queue).toString());
                    itemsBean.setVhost(namespaceName.getLocalName());
                    itemsBean.setDurable(true);
                    itemsBean.setExclusive(declareParams.isExclusive());
                    itemsBean.setAuto_delete(declareParams.isAutoDelete());
                    itemsBean.setArguments(declareParams.getArguments());

                    QueuesList.ItemsBean.MessageStatsBean statsBean = new QueuesList.ItemsBean.MessageStatsBean();
                    statsBean.setAck_details(new QueuesList.RateBean());
                    statsBean.setDeliver_details(new QueuesList.RateBean());
                    statsBean.setDeliver_get_details(new QueuesList.RateBean());
                    statsBean.setGet_details(new QueuesList.RateBean());
                    statsBean.setDeliver_no_ack_details(new QueuesList.RateBean());
                    statsBean.setGet_no_ack_details(new QueuesList.RateBean());
                    statsBean.setRedeliver_details(new QueuesList.RateBean());
                    statsBean.setPublish_details(new QueuesList.RateBean());
                    statsBean.setAck_details(new QueuesList.RateBean());

                    itemsBean.setMessage_stats(statsBean);
                    itemsBean.setMessages_details(new QueuesList.RateBean());
                    itemsBean.setMessages_ready_details(new QueuesList.RateBean());
                    itemsBean.setMessages_unacknowledged_details(
                            new QueuesList.RateBean());
                    itemsBean.setReductions_details(new QueuesList.RateBean());
                    return itemsBean;
                });
    }

    protected CompletableFuture<List<QueueBinds>> getQueueBindings(NamespaceName namespace, String queue) {
        return getTopicProperties(namespace.toString(), PersistentQueue.TOPIC_PREFIX, queue).thenCompose(properties -> {
            Set<PersistentExchange.Binding> bindings = Sets.newHashSet();
            try {
                if (properties.containsKey("BINDINGS")) {
                    List<PersistentExchange.Binding> amqpQueueProperties =
                            JSON_MAPPER.readValue(properties.get("BINDINGS"), new TypeReference<>() {
                            });
                    bindings.addAll(amqpQueueProperties);
                }
            } catch (JsonProcessingException e) {
                return FutureUtil.failedFuture(e);
            }
            List<QueueBinds> binds = bindings.stream()
                    .map(binding -> {
                        QueueBinds queueBinds = new QueueBinds();
                        queueBinds.setSource(binding.getSource());
                        queueBinds.setDestination(binding.getDes());
                        queueBinds.setRouting_key(binding.getKey());
                        queueBinds.setProperties_key(binding.getKey());
                        queueBinds.setVhost(namespace.getLocalName());
                        queueBinds.setDestination_type(binding.getDesType());
                        return queueBinds;
                    }).collect(Collectors.toList());
            return CompletableFuture.completedFuture(binds);
        }).exceptionally(throwable -> {
            log.error("Failed to save binding metadata for bind operation.", throwable);
            return null;
        });
    }

    protected CompletableFuture<List<MessageBean>> getQueueMessageAsync(String vhost, String queue,
                                                                        MessageParams messageParams) {
        PulsarAdmin adminClient = pulsarAdmin();
        String messageId = messageParams.getMessageId();
        String topicName = TopicUtil.getTopicName(PersistentQueue.TOPIC_PREFIX, tenant, vhost, queue);
        if (StringUtils.isNotBlank(messageId)) {
            StringTokenizer tokenizer = new StringTokenizer(messageId, ":", false);
            Pair<Long, Long> pair = switch (tokenizer.countTokens()) {
                case 2, 3 -> Pair.of(Long.valueOf(tokenizer.nextToken()), Long.valueOf(tokenizer.nextToken()));
                default -> {
                    throw new AoPServiceRuntimeException.GetMessageException("Unexpected messageId: " + messageId);
                }
            };
            return adminClient.topics()
                    .getMessageByIdAsync(topicName, pair.getKey(), pair.getValue())
                    .thenApply(message -> {
                        if (message == null) {
                            return Lists.newArrayList();
                        }
                        MessageBean messageBean = getMessageBean(messageParams, message);
                        return Lists.newArrayList(messageBean);
                    });
        }
        if (StringUtils.isAnyBlank(messageParams.getStartTime(), messageParams.getEndTime())) {
            throw new AoPServiceRuntimeException.GetMessageException("The start time and end time are required");
        }
        long startTime = LocalDateTime.parse(messageParams.getStartTime())
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long endTime = LocalDateTime.parse(messageParams.getEndTime())
                .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        if (startTime > endTime) {
            throw new AoPServiceRuntimeException.GetMessageException(
                    "The start time cannot be later than the end time");
        }
        if (startTime > System.currentTimeMillis()) {
            return CompletableFuture.completedFuture(Lists.newArrayList());
        }
        // by time query
        try (Reader<byte[]> reader = pulsarClient().newReader()
                .startMessageId(MessageId.earliest)
                .topic(topicName)
                .create()) {
            reader.seek(startTime);
            List<MessageBean> messageBeans = Lists.newArrayList();
            while (reader.hasMessageAvailable()) {
                Message<byte[]> message = reader.readNext(5, TimeUnit.SECONDS);
                // The default value is 200
                if (message == null || message.getPublishTime() > endTime
                        || messageBeans.size() >= 200
                        || (messageParams.getMessages() > 0 && messageBeans.size() >= messageParams.getMessages())) {
                    break;
                }
                MessageBean messageBean = getMessageBean(messageParams, message);
                messageBeans.add(messageBean);
            }
            return CompletableFuture.completedFuture(messageBeans);
        } catch (Exception e) {
            throw new AoPServiceRuntimeException.GetMessageException(e);
        }
    }

    @NotNull
    private static MessageBean getMessageBean(MessageParams messageParams, Message<byte[]> message) {
        MessageBean messageBean = new MessageBean();
        if ("base64".equals(messageParams.getEncoding())) {
            byte[] encode = Base64.getEncoder().encode(message.getValue());
            messageBean.setPayload(new String(encode));
        } else {
            messageBean.setPayload(new String(message.getValue()));
        }
        Map<String, String> properties = message.getProperties();
        messageBean.setPayload_bytes(message.getData().length);
        messageBean.setRedelivered(false);
        messageBean.setPayload_encoding("string");
        messageBean.setRouting_key(properties.get(MessageConvertUtils.PROP_ROUTING_KEY));
        messageBean.setExchange(properties.get(MessageConvertUtils.PROP_EXCHANGE));
        Map<String, Object> props = new HashMap<>();
        Map<String, Object> propsHeaders = new HashMap<>();
        propsHeaders.put("pulsar_message_position", message.getMessageId().toString());
        props.put("headers", propsHeaders);
        properties.forEach((k, v) -> {
            if (k.startsWith(MessageConvertUtils.BASIC_PROP_PRE)) {
                props.put(k.substring(MessageConvertUtils.BASIC_PROP_PRE.length()), v);
            } else if (k.startsWith(MessageConvertUtils.BASIC_PROP_HEADER_PRE)) {
                propsHeaders.put(k.substring(MessageConvertUtils.BASIC_PROP_HEADER_PRE.length()), v);
            }
        });
        messageBean.setProperties(props);
        return messageBean;
    }

    protected CompletableFuture<Void> purgeQueueAsync(String vhost, String queue, PurgeQueueParams purgeQueueParams) {
        if (!"purge".equals(purgeQueueParams.getMode())) {
            return CompletableFuture.completedFuture(null);
        }
        return aop().getBrokerService()
                .getTopic(TopicUtil.getTopicName(PersistentQueue.TOPIC_PREFIX, tenant, vhost, queue), false)
                .thenAccept(topicOptional -> topicOptional.ifPresent(topic -> {
                    if (topic instanceof PersistentTopic persistentTopic) {
                        persistentTopic.clearBacklog();
                    }
                }))
                .thenCompose(__ -> queueContainer().asyncGetQueue(getNamespaceName(vhost), queue, false)
                        .thenAccept(amqpQueue -> {
                            if (amqpQueue instanceof PersistentQueue persistentQueue) {
                                persistentQueue.start();
                            }
                        }));
    }

    protected CompletableFuture<Void> startExpirationDetection(String vhost, String queue) {
        return queueContainer().asyncGetQueue(getNamespaceName(vhost), queue, false)
                .thenAccept(amqpQueue -> {
                    if (amqpQueue instanceof PersistentQueue persistentQueue) {
                        persistentQueue.start();
                    }
                });
    }

    protected CompletableFuture<AmqpQueue> loadQueueAsync(String vhost, String queue) {
        return queueContainer().asyncGetQueue(getNamespaceName(vhost), queue, false);
    }

    protected CompletableFuture<QueueDetail> getQueueDetailAsync(String vhost, String queue, long age, long incr) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        PrometheusAdmin prometheusAdmin = prometheusAdmin();
        CompletableFuture<Map<String, QueueRangeMetrics>> queueDetailMetrics;
        if (prometheusAdmin.isConfig()) {
            queueDetailMetrics =
                    prometheusAdmin.queryRangeQueueDetailMetrics(
                            namespaceName.getTenant() + "/" + namespaceName.getLocalName(), queue, age, incr);
        } else {
            queueDetailMetrics = CompletableFuture.completedFuture(null);
        }

        return queueContainer().asyncGetQueue(namespaceName, queue, false)
                .thenApply(qu -> {
                    if (qu == null) {
                        throw new AoPServiceRuntimeException.NoSuchQueueException("Queue [" + queue + "] not created");
                    }
                    QueueDetail queueDetail = new QueueDetail();

                    Topic topic = qu.getTopic();
                    Subscription subscription = topic.getSubscriptions().get(PersistentQueue.DEFAULT_SUBSCRIPTION);
                    TopicStatsImpl stats = topic.getStats(false, false, false);

                    SubscriptionStats subscriptionStats = stats.getSubscriptions().get("AMQP_DEFAULT");
                    if (qu instanceof PersistentQueue persistentQueue) {
                        queueDetail.setName(queue);
                        queueDetail.setFullName(TopicName.get(TopicDomain.persistent.value(), namespaceName,
                                PersistentQueue.TOPIC_PREFIX + queue).toString());
                        queueDetail.setDurable(persistentQueue.getDurable());
                        queueDetail.setExclusive(persistentQueue.getExclusive());
                        queueDetail.setArguments(persistentQueue.getArguments());
                        queueDetail.setAuto_delete(persistentQueue.getAutoDelete());
                        queueDetail.setVhost(vhost);

                        queueDetail.setBacking_queue_status(new HashMap<>());
                        queueDetail.setDeliveries(Lists.newArrayList());
                        queueDetail.setEffective_policy_definition(new HashMap<>());
                        queueDetail.setGarbage_collection(new QueueDetail.GarbageCollectionBean());
                        queueDetail.setIdle_since(LocalDateTime.now().toString());
                        queueDetail.setIncoming(Lists.newArrayList());
                        queueDetail.setNode("");
                        // 监控
                        QueueDetail.MessageStatsBean messageStatsBean = new QueueDetail.MessageStatsBean();
                        QueueDetail.RateBean ackDetailsBean = new QueueDetail.RateBean();
                        ackDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean publishDetailsBean = new QueueDetail.RateBean();
                        publishDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean redeliverDetailsBean = new QueueDetail.RateBean();
                        redeliverDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean deliverDetailsBean = new QueueDetail.RateBean();
                        deliverDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean messagesDetailsBean = new QueueDetail.RateBean();
                        messagesDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean readyDetailsBean = new QueueDetail.RateBean();
                        readyDetailsBean.setSamples(
                                Lists.newArrayList(new SamplesBean(0, System.currentTimeMillis())));
                        List<QueueDetail.ConsumerDetailsBean> detailsBeans = Lists.newArrayList();
                        if (subscriptionStats != null) {
                            ackDetailsBean.setRate(subscriptionStats.getMessageAckRate());
                            publishDetailsBean.setRate(stats.getMsgRateIn());
                            redeliverDetailsBean.setRate(subscriptionStats.getMsgRateRedeliver());
                            deliverDetailsBean.setRate(subscriptionStats.getMsgRateOut());
                            messageStatsBean.setPublish(stats.getMsgInCounter());
                            messageStatsBean.setDeliver(subscriptionStats.getMsgOutCounter());
                            messagesDetailsBean.setRate(subscriptionStats.getMsgRateOut());
                            readyDetailsBean.setRate(stats.getMsgRateIn());

                            queueDetail.setMessages(stats.getMsgInCounter());
                            queueDetail.setMessages_ready(
                                    subscriptionStats.getMsgBacklog() - subscriptionStats.getUnackedMessages());
                            queueDetail.setMessages_unacknowledged(subscriptionStats.getUnackedMessages());
                            queueDetail.setConsumers(subscriptionStats.getConsumers().size());

                            if (subscription != null) {
                                List<Consumer> consumers = subscription.getConsumers();
                                consumers.stream()
                                        .map(consumer -> {
                                            ConsumerStats consumerStats = consumer.getStats();
                                            QueueDetail.ConsumerDetailsBean consumerDetailsBean =
                                                    new QueueDetail.ConsumerDetailsBean();
                                            consumerDetailsBean.setArguments(new HashMap<>());
                                            consumerDetailsBean.setConsumer_tag("");
                                            consumerDetailsBean.setExclusive(false);
                                            consumerDetailsBean.setPrefetch_count(consumerStats.getAvailablePermits());
                                            QueueDetail.ConsumerDetailsBean.QueueBean queueBean =
                                                    new QueueDetail.ConsumerDetailsBean.QueueBean();
                                            queueBean.setVhost(vhost);
                                            queueBean.setName(queue);
                                            consumerDetailsBean.setQueue(queueBean);
                                            consumerDetailsBean.setActivity_status("");
                                            QueueDetail.ConsumerDetailsBean.ChannelDetailsBean channelDetailsBean =
                                                    new QueueDetail.ConsumerDetailsBean.ChannelDetailsBean();
                                            channelDetailsBean.setUser("");
                                            channelDetailsBean.setNode("");
                                            channelDetailsBean.setPeer_host("");
                                            channelDetailsBean.setConnection_name("");
                                            String address = consumerStats.getAddress();
                                            if (consumer.getMetadata().containsKey("client_ip")) {
                                                address = consumer.getMetadata().get("client_ip");
                                            }
                                            channelDetailsBean.setName(
                                                    address + " -> " + topic.getBrokerService().getPulsar()
                                                            .getAdvertisedAddress());
                                            consumerDetailsBean.setChannel_details(channelDetailsBean);
                                            return consumerDetailsBean;
                                        }).collect(Collectors.toCollection(() -> detailsBeans));
                            }
                        }
                        queueDetail.setConsumer_details(detailsBeans);
                        messageStatsBean.setPublish_details(publishDetailsBean);
                        messageStatsBean.setAck_details(ackDetailsBean);
                        messageStatsBean.setDeliver_details(deliverDetailsBean);
                        messageStatsBean.setGet_details(new QueueDetail.RateBean());
                        messageStatsBean.setRedeliver_details(redeliverDetailsBean);
                        messageStatsBean.setDeliver_get_details(new QueueDetail.RateBean());
                        messageStatsBean.setDeliver_no_ack_details(new QueueDetail.RateBean());
                        messageStatsBean.setGet_no_ack_details(new QueueDetail.RateBean());
                        queueDetail.setMessage_stats(messageStatsBean);

                        queueDetail.setMessages_details(messagesDetailsBean);
                        queueDetail.setMessages_ready_details(readyDetailsBean);
                        queueDetail.setMessages_unacknowledged_details(new QueueDetail.RateBean());
                        queueDetail.setReductions_details(new QueueDetail.RateBean());
                    }
                    return queueDetail;
                })
                .thenCompose(queueDetail -> queueDetailMetrics.thenApply(map -> {
                    QueueRangeMetrics rangeMetrics;
                    if (map != null && (rangeMetrics = map.get(queueDetail.getFullName())) != null) {
                        queueDetail.getMessages_ready_details().setSamples(rangeMetrics.getReady());
                        queueDetail.getMessages_unacknowledged_details().setSamples(rangeMetrics.getUnAck());
                        queueDetail.getMessages_details().setSamples(rangeMetrics.getTotal());

                        QueueDetail.MessageStatsBean messageStats = queueDetail.getMessage_stats();
                        // Pulsar is Rate, it should be a value statistic.
                        // messageStats.getAck_details().setSamples(rangeMetrics.getAck());
                        messageStats.getPublish_details().setSamples(rangeMetrics.getPublish());
                        messageStats.getDeliver_details().setSamples(rangeMetrics.getDeliver());
                    }
                    return queueDetail;
                }));
    }

    protected CompletableFuture<QueueBean> getQueueBeanAsync(String vhost, String queue) {
        return queueContainer().asyncGetQueue(
                NamespaceName.get(tenant, vhost), queue, false).thenApply(qu -> {
            QueueBean bean = new QueueBean();
            bean.setName(queue);
            bean.setVhost(vhost);
            bean.setDurable(true);
            bean.setExclusive(qu.isExclusive());
            bean.setAutoDelete(qu.isAutoDelete());
            return bean;
        });
    }

    protected CompletableFuture<AmqpQueue> declareQueueAsync(NamespaceName namespaceName, String queue,
                                                             QueueDeclareParams declareParams) {
        return queueService().queueDeclare(namespaceName, queue, declareParams.isPassive(),
                declareParams.isDurable(), declareParams.isExclusive(), declareParams.isAutoDelete(),
                true, declareParams.getArguments(), -1);
    }

    protected CompletableFuture<Void> deleteQueueAsync(NamespaceName namespaceName, String queue, boolean ifUnused,
                                                       boolean ifEmpty) {
        return queueService().queueDelete(namespaceName, queue, ifUnused, ifEmpty, -1);
    }

}
