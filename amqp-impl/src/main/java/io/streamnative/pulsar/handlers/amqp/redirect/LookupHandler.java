package io.streamnative.pulsar.handlers.amqp.redirect;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Slf4j
public class LookupHandler {

    private PulsarClientImpl pulsarClient;

    public LookupHandler(RedirectConfiguration redirectConfig) throws PulsarClientException {
        pulsarClient = (PulsarClientImpl) PulsarClient.builder()
                .serviceUrl(redirectConfig.getBrokerServiceURL())
                .build();
    }

    public Pair<InetSocketAddress, InetSocketAddress> handleLookup(NamespaceName namespaceName)
            throws ExecutionException, InterruptedException, RedirectException {
        CompletableFuture<List<String>> completeFuture = this.pulsarClient.getLookup().
                getTopicsUnderNamespace(namespaceName, PulsarApi.CommandGetTopicsOfNamespace.Mode.ALL);
        List<String> topics = completeFuture.get();
        if (topics == null || topics.isEmpty()) {
            throw new RedirectException("The namespace has no topics.");
        }
        TopicName topicName = TopicName.get(topics.get(0));
        CompletableFuture<Pair<InetSocketAddress, InetSocketAddress>> completableFuture =
                this.pulsarClient.getLookup().getBroker(topicName);
        return completableFuture.get();
    }

}
