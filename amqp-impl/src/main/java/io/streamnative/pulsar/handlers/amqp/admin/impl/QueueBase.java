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

import io.streamnative.pulsar.handlers.amqp.AmqpQueue;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.VhostBean;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * QueueBase.
 */
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

    protected CompletableFuture<AmqpQueue> declareQueueAsync(String vhost, String queue,
                                                           QueueDeclareParams declareParams) {
        return queueService().queueDeclare(NamespaceName.get(tenant, vhost), queue, false,
                declareParams.isDurable(), declareParams.isExclusive(), declareParams.isAutoDelete(),
                true, declareParams.getArguments(), -1);
    }

    protected CompletableFuture<Void> deleteQueueAsync(String vhost, String queue, boolean ifUnused,
                                                       boolean ifEmpty) {
        return queueService().queueDelete(NamespaceName.get(tenant, vhost), queue, ifUnused, ifEmpty, -1);
    }

}
