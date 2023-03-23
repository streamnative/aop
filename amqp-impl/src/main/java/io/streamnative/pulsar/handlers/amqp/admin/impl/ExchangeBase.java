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

import com.google.common.collect.Lists;
import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.PublishParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.ExchangeDetail;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.ExchangeSource;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.ExchangesList;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil;
import io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils;
import io.streamnative.pulsar.handlers.amqp.utils.TopicUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Exchange base.
 */
@Slf4j
public class ExchangeBase extends BaseResources {

    protected CompletableFuture<String> asyncPublish(NamespaceName namespaceName, String exchange,
                                                     PublishParams params) {
        return exchangeContainer().asyncGetExchange(namespaceName, exchange, false, null)
                .thenCompose(amqpExchange -> {
                    if (amqpExchange == null) {
                        throw new AoPServiceRuntimeException.NoSuchExchangeException(
                                "Exchange [" + exchange + "] not created");
                    }
                    MessageImpl<byte[]> message = MessageConvertUtils.toPulsarMessage(params);
                    PulsarClient client;
                    try {
                        client = aop().getBrokerService().getPulsar().getClient();
                    } catch (PulsarServerException e) {
                        throw new AoPServiceRuntimeException(e);
                    }
                    return client.newProducer()
                            .topic(TopicUtil.getTopicName(PersistentExchange.TOPIC_PREFIX, namespaceName, exchange))
                            .enableBatching(false)
                            .createAsync()
                            .thenCompose(producer -> producer.newMessage()
                                    .value(message.getData())
                                    .properties(message.getProperties())
                                    .sendAsync()
                                    .thenAccept(position -> log.info("Publish message success, position {}", position))
                                    .thenRun(producer::closeAsync));
                })
                .thenApply(__ -> "{\"routed\":true}");
    }

    protected CompletableFuture<List<ExchangeBean>> getExchangeListAsync() {
        final List<ExchangeBean> exchangeList = new ArrayList<>();
        return namespaceResource().listNamespacesAsync(tenant)
                .thenCompose(nsList -> {
                    Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
                    for (String ns : nsList) {
                        futureList.add(getExchangeListByVhostAsync(ns).thenAccept(exchangeList::addAll));
                    }
                    return FutureUtil.waitForAll(futureList);
                }).thenApply(__ -> exchangeList);
    }

    private CompletableFuture<List<String>> getExchangeListAsync(String tenant, String ns) {
        return namespaceService()
                .getFullListOfTopics(NamespaceName.get(tenant, ns))
                .thenApply(list -> list.stream().filter(s ->
                        s.contains(PersistentExchange.TOPIC_PREFIX)).collect(Collectors.toList()));
    }

    protected CompletableFuture<List<ExchangesList.ItemsBean>> getExchangeListByNamespaceAsync(String vhost) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        return getExchangeListAsync(namespaceName.getTenant(), namespaceName.getLocalName())
                .thenCompose(exList -> {
                    List<ExchangesList.ItemsBean> itemsBeans = new CopyOnWriteArrayList<>();
                    Stream<CompletableFuture<ExchangesList.ItemsBean>> futureStream =
                            exList.stream().map(topic -> {
                                String exchangeName = TopicName.get(topic).getLocalName()
                                        .substring(PersistentExchange.TOPIC_PREFIX.length());
                                return getTopicProperties(namespaceName.toString(), PersistentExchange.TOPIC_PREFIX,
                                        exchangeName).thenApply(properties -> {
                                    ExchangeDeclareParams exchangeDeclareParams =
                                            ExchangeUtil.covertMapAsParams(properties);
                                    ExchangesList.ItemsBean itemsBean = new ExchangesList.ItemsBean();
                                    itemsBean.setName(exchangeName);
                                    itemsBean.setDurable(exchangeDeclareParams.isDurable());
                                    itemsBean.setInternal(exchangeDeclareParams.isInternal());
                                    itemsBean.setType(exchangeDeclareParams.getType());
                                    itemsBean.setArguments(exchangeDeclareParams.getArguments());
                                    itemsBean.setAuto_delete(exchangeDeclareParams.isAutoDelete());
                                    itemsBean.setVhost(vhost);
                                    // 指标
                                    ExchangesList.ItemsBean.MessageStatsBean messageStatsBean =
                                            new ExchangesList.ItemsBean.MessageStatsBean();
                                    ExchangesList.ItemsBean.MessageStatsBean.PublishOutDetailsBean
                                            publishOutDetailsBean =
                                            new ExchangesList.ItemsBean.MessageStatsBean.PublishOutDetailsBean();
                                    messageStatsBean.setPublish_out_details(publishOutDetailsBean);
                                    ExchangesList.ItemsBean.MessageStatsBean.PublishInDetailsBean
                                            publishInDetailsBean =
                                            new ExchangesList.ItemsBean.MessageStatsBean.PublishInDetailsBean();
                                    messageStatsBean.setPublish_in_details(publishInDetailsBean);
                                    itemsBean.setMessage_stats(messageStatsBean);
                                    itemsBeans.add(itemsBean);
                                    return itemsBean;
                                });
                            });
                    List<CompletableFuture<ExchangesList.ItemsBean>> futures =
                            futureStream.collect(Collectors.toList());
                    return FutureUtil.waitForAll(futures).thenApply(__ -> {
                        itemsBeans.sort(Comparator.comparing(ExchangesList.ItemsBean::getName));
                        return itemsBeans;
                    });
                });
    }

    protected CompletableFuture<List<ExchangeBean>> getExchangeListByVhostAsync(String vhost) {
        return getExchangeListAsync(tenant, vhost).thenCompose(exList -> {
            Collection<CompletableFuture<Void>> futureList = new ArrayList<>();
            List<ExchangeBean> beanList = new ArrayList<>();
            exList.forEach(topic -> {
                String exchangeName = TopicName.get(topic).getLocalName()
                        .substring(PersistentExchange.TOPIC_PREFIX.length());
                futureList.add(getExchangeBeanAsync(vhost, exchangeName).thenAccept(beanList::add));
            });
            return FutureUtil.waitForAll(futureList).thenApply(__ -> beanList);
        });
    }

    protected CompletableFuture<ExchangeBean> getExchangeBeanAsync(String vhost, String exchangeName) {
        return exchangeContainer().asyncGetExchange(
                NamespaceName.get(tenant, vhost), exchangeName, false, null).thenApply(ex -> {
            ExchangeBean exchangeBean = new ExchangeBean();
            exchangeBean.setName(exchangeName);
            exchangeBean.setType(ex.getType().toString().toLowerCase());
            exchangeBean.setVhost(vhost);
            exchangeBean.setAutoDelete(ex.getAutoDelete());
            exchangeBean.setDurable(ex.getDurable());
            exchangeBean.setInternal(ex.getInternal());
            exchangeBean.setArguments(ex.getArguments());
            return exchangeBean;
        });
    }

    protected CompletableFuture<ExchangeDetail> getExchangeDetailAsync(String vhost, String exchangeName) {
        return exchangeContainer().asyncGetExchange(
                getNamespaceName(vhost), exchangeName, false, null).thenApply(ex -> {
            ExchangeDetail exchangeBean = new ExchangeDetail();
            Topic exchangeTopic = ex.getTopic();
            TopicStatsImpl stats = exchangeTopic.getStats(false, false, false);
            if (ex instanceof PersistentExchange persistentExchange) {
                exchangeBean.setName(exchangeName);
                exchangeBean.setType(persistentExchange.getType().toString().toLowerCase());
                exchangeBean.setVhost(vhost);
                exchangeBean.setAuto_delete(persistentExchange.getAutoDelete());
                exchangeBean.setDurable(persistentExchange.getDurable());
                exchangeBean.setInternal(persistentExchange.getInternal());
                exchangeBean.setArguments(persistentExchange.getArguments());
                // 监控
                exchangeBean.setIncoming(Lists.newArrayList());
                exchangeBean.setOutgoing(Lists.newArrayList());
                ExchangeDetail.MessageStatsBean messageStatsBean = new ExchangeDetail.MessageStatsBean();
                ExchangeDetail.MessageStatsBean.PublishInDetailsBean publishInDetailsBean =
                        new ExchangeDetail.MessageStatsBean.PublishInDetailsBean();
                ExchangeDetail.MessageStatsBean.PublishOutDetailsBean publishOutDetailsBean =
                        new ExchangeDetail.MessageStatsBean.PublishOutDetailsBean();
                publishInDetailsBean.setRate(stats.getMsgRateIn());
                ExchangeDetail.MessageStatsBean.PublishInDetailsBean.SamplesBean samplesBean =
                        new ExchangeDetail.MessageStatsBean.PublishInDetailsBean.SamplesBean();
                samplesBean.setTimestamp(System.currentTimeMillis());
                publishInDetailsBean.setSamples(Lists.newArrayList(samplesBean));
                // --
                ExchangeDetail.MessageStatsBean.PublishOutDetailsBean.SamplesBeanX samplesBeanX =
                        new ExchangeDetail.MessageStatsBean.PublishOutDetailsBean.SamplesBeanX();
                samplesBeanX.setTimestamp(System.currentTimeMillis());
                publishOutDetailsBean.setSamples(Lists.newArrayList(samplesBeanX));
                publishOutDetailsBean.setRate(stats.getMsgRateOut());
                messageStatsBean.setPublish_in_details(publishInDetailsBean);
                messageStatsBean.setPublish_out_details(publishOutDetailsBean);
                messageStatsBean.setPublish_in(stats.getMsgInCounter());
                messageStatsBean.setPublish_out(stats.getMsgOutCounter());
                exchangeBean.setMessage_stats(messageStatsBean);
            }
            return exchangeBean;
        });
    }

    protected CompletableFuture<List<ExchangeSource>> getExchangeSourceAsync(String vhost, String exchangeName) {
        return exchangeContainer().asyncGetExchange(
                getNamespaceName(vhost), exchangeName, false, null).thenApply(ex -> {
            List<ExchangeSource> exchangeSources = new ArrayList<>();
            if (ex instanceof PersistentExchange persistentExchange) {
                ExchangeSource exchangeBean = new ExchangeSource();
                exchangeBean.setVhost(vhost);
                exchangeBean.setSource(exchangeName);
                persistentExchange.getBindings()
                        .stream()
                        .map(binding -> {
                            exchangeBean.setArguments(binding.getArguments());
                            exchangeBean.setDestination(binding.getDes());
                            exchangeBean.setRouting_key(binding.getKey());
                            exchangeBean.setDestination_type(binding.getDesType());
                            exchangeBean.setProperties_key(binding.getKey());
                            return exchangeBean;
                        }).collect(Collectors.toCollection(() -> exchangeSources));
            }
            return exchangeSources;
        });
    }

    protected CompletableFuture<AmqpExchange> declareExchange(NamespaceName namespaceName, String exchangeName,
                                                              ExchangeDeclareParams declareParams) {
        return exchangeService().exchangeDeclare(namespaceName, exchangeName,
                declareParams.getType(), declareParams.isPassive(), declareParams.isDurable(),
                declareParams.isAutoDelete(), declareParams.isInternal(), declareParams.getArguments());
    }

    protected CompletableFuture<Void> deleteExchange(NamespaceName namespaceName, String exchangeName,
                                                     boolean ifUnused) {
        return exchangeService().exchangeDelete(namespaceName, exchangeName, ifUnused);
    }

}
