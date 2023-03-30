package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import java.util.List;
import lombok.Data;

@Data
public class ExchangeRangeMetrics {

    private String topic;

    private List<SamplesBean> in;
    private List<SamplesBean> out;

    private double inRate;
    private double outRate;

}
