package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import lombok.Data;

@Data
public class QueueListMetrics {

    private String topicName;

    private long ready;
    private long unAck;
    private long total;

    private double incomingRate;
    private double deliverRate;
    private double ackRate;
}
