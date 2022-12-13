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

import io.streamnative.pulsar.handlers.amqp.AmqpMessageRouter;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingBean;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.core.Response;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

/**
 * BindingBase.
 */
public class BindingBase extends BaseResources {

    protected CompletableFuture<BindingBean> getBindingsByPropsKeyAsync(String vhost, String exchange, String queue,
                                                                        String propsKey) {
        return getBindingsAsync(vhost, exchange, queue, propsKey).thenApply(list -> {
            if (list.size() == 0) {
                throw new RestException(Response.Status.NOT_FOUND.getStatusCode(), "Object Not Found");
            }
            return list.get(0);
        });
    }

    protected CompletableFuture<List<BindingBean>> getBindingsAsync(String vhost, String exchange, String queue,
                                                                    String propsKey) {
        List<BindingBean> beans = new ArrayList<>();
        return queueContainer().asyncGetQueue(NamespaceName.get(tenant, vhost), queue, false)
                .thenAccept(amqpQueue -> {
                    if (amqpQueue == null) {
                        throw new RestException(Response.Status.NOT_FOUND.getStatusCode(), "Object Not Found");
                    }
                    AmqpMessageRouter router = amqpQueue.getRouter(exchange);
                    if (router == null) {
                        return;
                    }
                    for (String key : router.getBindingKey()) {
                        if (propsKey != null && !propsKey.equals(key)) {
                            continue;
                        }
                        BindingBean bean = new BindingBean();
                        bean.setVhost(vhost);
                        bean.setSource(router.getExchange().getName());
                        bean.setDestination(queue);
                        bean.setRoutingKey(key);
                        bean.setPropertiesKey(key);
                        bean.setDestinationType("queue");
                        beans.add(bean);
                    }
                }).thenApply(__ -> beans);
    }

    protected CompletableFuture<Void> queueBindAsync(String vhost, String exchange, String queue,
                                                     BindingParams params) {
        if (aop().getAmqpConfig().isAmqpMultiBundleEnable()) {
            return exchangeService().queueBind(NamespaceName.get(tenant, vhost), exchange, queue,
                    params.getRoutingKey(), params.getArguments());
        } else {
            return queueService().queueBind(NamespaceName.get(tenant, vhost), queue, exchange, params.getRoutingKey(),
                    false, FieldTable.convertToFieldTable(params.getArguments()), -1);
        }
    }

    protected CompletableFuture<Void> queueUnbindAsync(String vhost, String exchange, String queue,
                                                       String propertiesKey) {
        return queueService().queueUnbind(NamespaceName.get(tenant, vhost), queue, exchange, propertiesKey, null, -1);
    }

}
