package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import lombok.Data;

@Data
public class ExchangeListMetrics {

    private String topicName;

    private double inRate;
    private double outRate;
}
