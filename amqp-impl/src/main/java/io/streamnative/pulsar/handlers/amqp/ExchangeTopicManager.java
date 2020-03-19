package io.streamnative.pulsar.handlers.amqp;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ExchangeTopicManager {

    private final ConcurrentHashMap<String, CompletableFuture<PersistentTopic>> topics;

    ExchangeTopicManager() {
        topics = new ConcurrentHashMap<>();
    }

    public CompletableFuture<PersistentTopic> getTopic(String exchangeName) {
        return null;
    }

}
