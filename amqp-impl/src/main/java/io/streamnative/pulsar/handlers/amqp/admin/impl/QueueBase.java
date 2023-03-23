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
import static org.apache.pulsar.common.util.Codec.decode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.ExchangesList;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueueBinds;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueueDetail;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueuesList;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import io.streamnative.pulsar.handlers.amqp.utils.QueueUtil;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.MetaStore;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.Stat;

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

    private CompletableFuture<List<String>> getQueueListAsync(String tenant, String ns) {
        return namespaceService()
                .getFullListOfTopics(NamespaceName.get(tenant, ns))
                .thenApply(list -> list.stream().filter(s ->
                        s.contains(PersistentQueue.TOPIC_PREFIX)).collect(Collectors.toList()));
    }

    protected CompletableFuture<List<QueuesList.ItemsBean>> getQueueListByNamespaceAsync(String vhost) {
        NamespaceName namespaceName = NamespaceName.get(vhost);
        return getQueueListAsync(namespaceName.getTenant(), namespaceName.getLocalName())
                .thenCompose(exList -> {
                    Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
                    List<QueuesList.ItemsBean> beanList = new CopyOnWriteArrayList<>();
                    exList.forEach(topic -> {
                        String queue = TopicName.get(topic).getLocalName()
                                .substring(PersistentQueue.TOPIC_PREFIX.length());
                        futureList.add(getQueueBeanByNamespaceAsync(vhost, queue).thenAccept(
                                beanList::add));
                    });
                    return FutureUtil.waitForAll(futureList).thenApply(__ -> {
                        beanList.sort(Comparator.comparing(QueuesList.ItemsBean::getName));
                        return beanList;
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

    protected CompletableFuture<QueuesList.ItemsBean> getQueueBeanByNamespaceAsync(String namespaceName,
                                                                                   String queue) {
        return getTopicProperties(namespaceName, PersistentQueue.TOPIC_PREFIX, queue).thenApply(properties -> {
            QueueDeclareParams declareParams = QueueUtil.covertMapAsParams(properties);
            QueuesList.ItemsBean itemsBean = new QueuesList.ItemsBean();
            itemsBean.setName(queue);
            itemsBean.setVhost(namespaceName);
            itemsBean.setDurable(true);
            itemsBean.setExclusive(declareParams.isExclusive());
            itemsBean.setAuto_delete(declareParams.isAutoDelete());
            itemsBean.setArguments(declareParams.getArguments());
            QueuesList.ItemsBean.MessageStatsBean statsBean = new QueuesList.ItemsBean.MessageStatsBean();
            statsBean.setAck_details(new QueuesList.ItemsBean.MessageStatsBean.AckDetailsBean());
            QueuesList.ItemsBean.MessageStatsBean.DeliverDetailsBean deliverDetailsBean =
                    new QueuesList.ItemsBean.MessageStatsBean.DeliverDetailsBean();
            statsBean.setDeliver_details(deliverDetailsBean);
            statsBean.setGet_details(new QueuesList.ItemsBean.MessageStatsBean.GetDetailsBean());
            statsBean.setDeliver_no_ack_details(new QueuesList.ItemsBean.MessageStatsBean.DeliverNoAckDetailsBean());
            statsBean.setGet_no_ack_details(new QueuesList.ItemsBean.MessageStatsBean.GetNoAckDetailsBean());
            statsBean.setRedeliver_details(new QueuesList.ItemsBean.MessageStatsBean.RedeliverDetailsBean());

            QueuesList.ItemsBean.MessageStatsBean.PublishDetailsBean publishDetailsBean =
                    new QueuesList.ItemsBean.MessageStatsBean.PublishDetailsBean();
            statsBean.setPublish_details(publishDetailsBean);

            QueuesList.ItemsBean.MessageStatsBean.AckDetailsBean ackDetailsBean =
                    new QueuesList.ItemsBean.MessageStatsBean.AckDetailsBean();
            statsBean.setAck_details(ackDetailsBean);

            itemsBean.setMessage_stats(statsBean);
            itemsBean.setMessages_details(new QueuesList.ItemsBean.MessagesDetailsBean());
            itemsBean.setMessages_ready_details(new QueuesList.ItemsBean.MessagesReadyDetailsBean());
            itemsBean.setMessages_unacknowledged_details(new QueuesList.ItemsBean.MessagesUnacknowledgedDetailsBean());
            itemsBean.setReductions_details(new QueuesList.ItemsBean.ReductionsDetailsBean());
            return itemsBean;
        });
    }

    protected CompletableFuture<List<QueueBinds>> getQueueBindings(String vhost, String queue) {
        return getTopicProperties(vhost, PersistentQueue.TOPIC_PREFIX, queue).thenCompose(properties -> {
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
                        queueBinds.setVhost(vhost);
                        queueBinds.setDestination_type(binding.getDesType());
                        return queueBinds;
                    }).collect(Collectors.toList());
            return CompletableFuture.completedFuture(binds);
        }).exceptionally(throwable -> {
            log.error("Failed to save binding metadata for bind operation.", throwable);
            return null;
        });
    }

    protected CompletableFuture<QueueDetail> getQueueDetailAsync(String vhost, String queue) {
        return queueContainer().asyncGetQueue(NamespaceName.get(vhost), queue, false)
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
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean publishDetailsBean = new QueueDetail.RateBean();
                        publishDetailsBean.setSamples(
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean redeliverDetailsBean = new QueueDetail.RateBean();
                        redeliverDetailsBean.setSamples(
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean deliverDetailsBean = new QueueDetail.RateBean();
                        deliverDetailsBean.setSamples(
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean messagesDetailsBean = new QueueDetail.RateBean();
                        messagesDetailsBean.setSamples(
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
                        QueueDetail.RateBean readyDetailsBean = new QueueDetail.RateBean();
                        readyDetailsBean.setSamples(
                                Lists.newArrayList(new QueueDetail.SamplesBean(0, System.currentTimeMillis())));
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

                            List<QueueDetail.ConsumerDetailsBean> detailsBeans = Lists.newArrayList();
                            if(subscription != null){
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

                            queueDetail.setConsumer_details(detailsBeans);
                        }
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
                });
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
        return queueService().queueDeclare(namespaceName, queue, false,
                        declareParams.isDurable(), declareParams.isExclusive(), declareParams.isAutoDelete(),
                        true, declareParams.getArguments(), -1);
    }

    protected CompletableFuture<Void> deleteQueueAsync(NamespaceName namespaceName, String queue, boolean ifUnused,
                                                       boolean ifEmpty) {
        return queueService().queueDelete(namespaceName, queue, ifUnused, ifEmpty, -1);
    }

}
