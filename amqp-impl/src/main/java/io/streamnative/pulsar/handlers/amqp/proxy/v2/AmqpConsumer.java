package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import lombok.Getter;
import org.apache.pulsar.client.api.Consumer;

public class AmqpConsumer {

    @Getter
    private final Consumer<byte[]> consumer;
    @Getter
    private final String consumerTag;
    @Getter
    private boolean autoAck = false;

    public AmqpConsumer(Consumer<byte[]> consumer, String consumerTag, boolean autoAck) {
        this.consumer = consumer;
        this.consumerTag = consumerTag;
        this.autoAck = autoAck;
    }

    public String getTopic() {
        return this.consumer.getTopic();
    }

}
