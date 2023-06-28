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
package io.streamnative.pulsar.handlers.amqp.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.QueueBinds;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.utils.HttpUtil;
import io.streamnative.pulsar.handlers.amqp.utils.JsonUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Amqp admin.
 */
public class AmqpAdmin {

    private final String baseUrl;

    public AmqpAdmin(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port + "/api";
    }

    public CompletableFuture<Void> exchangeDeclare(NamespaceName namespaceName,
                                                   String exchange,
                                                   ExchangeDeclareParams exchangeDeclareParams) {
        String url = String.format("%s/exchanges/%s/%s", baseUrl, namespaceName.getLocalName(), exchange);
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(exchangeDeclareParams),
                    Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueDeclare(NamespaceName namespaceName,
                                                String queue,
                                                QueueDeclareParams queueDeclareParams) {
        String url = String.format("%s/queues/%s/%s", baseUrl, namespaceName.getLocalName(), queue);
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(queueDeclareParams),
                    Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueDelete(NamespaceName namespaceName, String queue, Map<String, Object> params) {
        String url = String.format("%s/queues/%s/%s", baseUrl, namespaceName.getLocalName(), queue);
        return HttpUtil.deleteAsync(url, params, Map.of("tenant", namespaceName.getTenant()));
    }

    public CompletableFuture<Void> queuePurge(NamespaceName namespaceName, String queue, Map<String, Object> params) {
        String url = String.format("%s/queues/%s/%s/contents", baseUrl, namespaceName.getLocalName(), queue);
        return HttpUtil.deleteAsync(url, params, Map.of("tenant", namespaceName.getTenant()));
    }

    public CompletableFuture<Void> exchangeDelete(NamespaceName namespaceName, String exchange, Map<String, Object> params) {
        String url = String.format("%s/exchanges/%s/%s", baseUrl, namespaceName.getLocalName(), exchange);
        return HttpUtil.deleteAsync(url, params, Map.of("tenant", namespaceName.getTenant()));
    }

    public void startExpirationDetection(NamespaceName namespaceName, String queue) {
        String url = String.format("%s/queues/%s/%s/startExpirationDetection", baseUrl, namespaceName.getLocalName(), queue);
        HttpUtil.putAsync(url, new HashMap<>(1), Map.of("tenant", namespaceName.getTenant()));
    }

    public CompletableFuture<Void> queueBind(NamespaceName namespaceName,
                                             String exchange,
                                             String queue,
                                             BindingParams bindingParams) {
        String url = String.format("%s/bindings/%s/e/%s/q/%s", baseUrl, namespaceName.getLocalName(), exchange, queue);
        try {
            return HttpUtil.postAsync(url, JsonUtil.toMap(bindingParams), Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
    public CompletableFuture<Void> loadExchange(NamespaceName namespaceName, String ex) {
        String url = String.format("%s/exchanges/%s/%s/loadExchange", baseUrl, namespaceName.getLocalName(), ex);
        return HttpUtil.getAsync(url, Map.of("tenant", namespaceName.getTenant()));
    }
    public CompletableFuture<Void> loadQueue(NamespaceName namespaceName, String queue) {
        String url = String.format("%s/queues/%s/%s/loadQueue", baseUrl, namespaceName.getLocalName(), queue);
        return HttpUtil.getAsync(url, Map.of("tenant", namespaceName.getTenant()));
    }
    public CompletableFuture<Void> loadAllExchangeByVhost(NamespaceName namespaceName) {
        String url = String.format("%s/exchanges/%s/loadAllExchangeByVhost", baseUrl, namespaceName.getLocalName());
        return HttpUtil.getAsync(url, Map.of("tenant", namespaceName.getTenant()));
    }
    public CompletableFuture<Void> loadAllVhostForExchange(String tenant) {
        String url = String.format("%s/vhosts/loadAllVhostForExchange", baseUrl);
        return HttpUtil.getAsync(url, Map.of("tenant",tenant));
    }

    public CompletableFuture<List<QueueBinds>> getQueueBindings(NamespaceName namespaceName, String queue) {
        String url = String.format("%s/queues/%s/%s/bindings", baseUrl, namespaceName.getLocalName(), queue);
        return HttpUtil.getAsync(url, Map.of("tenant", namespaceName.getTenant()), new TypeReference<List<QueueBinds>>() {
        });
    }

    public CompletableFuture<Void> queueBindExchange(NamespaceName namespaceName,
                                                     String exchange,
                                                     String queue,
                                                     BindingParams bindingParams) {
        String url = String.format("%s/queueBindExchange/%s/e/%s/q/%s", baseUrl, namespaceName.getLocalName(), exchange,
                queue);
        try {
            return HttpUtil.postAsync(url, JsonUtil.toMap(bindingParams), Map.of("tenant", namespaceName.getTenant()));
        } catch (JsonProcessingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueUnBindExchange(NamespaceName namespaceName,
                                                       String exchange,
                                                       String queue,
                                                       String props) {
        String url = String.format("%s/queueUnBindExchange/%s/e/%s/q/%s/unbind",
                baseUrl, namespaceName.getLocalName(), exchange, queue);
        return HttpUtil.deleteAsync(url, Map.of("properties_key", props), Map.of("tenant", namespaceName.getTenant()));
    }

    public CompletableFuture<Void> queueUnbind(NamespaceName namespaceName,
                                               String exchange,
                                               String queue,
                                               String props) {
        String url = String.format("%s/bindings/%s/e/%s/q/%s/unbind",
                baseUrl, namespaceName.getLocalName(), exchange, queue);
        return HttpUtil.deleteAsync(url, Map.of("properties_key", props), Map.of("tenant", namespaceName.getTenant()));
    }
}
