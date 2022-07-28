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
package io.streamnative.pulsar.handlers.amqp.admin.impl;

import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ExchangeBase extends BaseResources {

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

    private CompletableFuture<List<VhostBean>> getVhostListAsync() {
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

    private CompletableFuture<List<String>> getExchangeListAsync(String tenant, String ns) {
        return namespaceService()
                .getFullListOfTopics(NamespaceName.get(tenant, ns))
                .thenApply(list -> list.stream().filter(s ->
                        s.contains(PersistentExchange.TOPIC_PREFIX)).collect(Collectors.toList()));
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
            exchangeBean.setInternal(false);
            return exchangeBean;
        });
    }

    protected CompletableFuture<AmqpExchange> declareExchange(String vhost, String exchange,
                                                              ExchangeDeclareParams declareParams) {
        return exchangeService().exchangeDeclare(NamespaceName.get(tenant, vhost), exchange, declareParams.getType(),
                false, declareParams.isDurable(), declareParams.isAuto_delete(), declareParams.isInternal(), null);
    }

    protected CompletableFuture<Void> deleteExchange(String vhost, String exchange, boolean ifUnused) {
        return exchangeService().exchangeDelete(NamespaceName.get(tenant, vhost), exchange, ifUnused);
    }

}
