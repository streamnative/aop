package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq.SamplesBean;
import java.util.List;
import lombok.Data;

@Data
public class QueueRangeMetrics {

    private String topic;

    private List<SamplesBean> ready;
    private List<SamplesBean> unAck;
    private List<SamplesBean> total;

    private List<SamplesBean> publish;
    private List<SamplesBean> deliver;
    private List<SamplesBean> ack;

    private double publishValue;
    private double deliverValue;
    private double ackValue;

}
