package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
public class SamplesBean {
    private double sample;
    private double timestamp;
}
