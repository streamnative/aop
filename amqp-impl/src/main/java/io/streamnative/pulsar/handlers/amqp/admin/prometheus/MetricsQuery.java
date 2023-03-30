package io.streamnative.pulsar.handlers.amqp.admin.prometheus;

import lombok.Data;

@Data
public class MetricsQuery {

    private String query;
    private Long start;
    private Long end;
    private Integer step;
}
