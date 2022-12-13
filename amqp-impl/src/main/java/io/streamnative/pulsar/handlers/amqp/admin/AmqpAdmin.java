package io.streamnative.pulsar.handlers.amqp.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.utils.HttpUtil;
import io.streamnative.pulsar.handlers.amqp.utils.JsonUtil;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Amqp admin.
 */
public class AmqpAdmin {

    private final String baseUrl;

    public AmqpAdmin(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port + "/api/";
    }

    public CompletableFuture<Void> exchangeDeclare(String vhost,
                                                   String exchange,
                                                   ExchangeDeclareParams exchangeDeclareParams) {
        NamespaceName namespaceName = NamespaceName.get(vhost);
        String url = baseUrl + "exchanges" + "/" + namespaceName.getLocalName() + "/" + exchange;
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(exchangeDeclareParams));
        } catch (JsonProcessingException e) {
            return  CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueDeclare(String vhost,
                                                String queue,
                                                QueueDeclareParams queueDeclareParams) {
        NamespaceName namespaceName = NamespaceName.get(vhost);
        String url = baseUrl + "queues" + "/" + namespaceName.getLocalName() + "/" + queue;
        try {
            return HttpUtil.putAsync(url, JsonUtil.toMap(queueDeclareParams));
        } catch (JsonProcessingException e) {
            return  CompletableFuture.failedFuture(e);
        }
    }

    public CompletableFuture<Void> queueBind(String vhost,
                                             String exchange,
                                             String queue,
                                             BindingParams bindingParams) {
        NamespaceName namespaceName = NamespaceName.get(vhost);
        String url = baseUrl + "bindings" + "/" + namespaceName.getLocalName() + "/e/" + exchange + "/q/" + queue;
        try {
            return HttpUtil.postAsync(url, JsonUtil.toMap(bindingParams));
        } catch (JsonProcessingException e) {
            return  CompletableFuture.failedFuture(e);
        }
    }

}
