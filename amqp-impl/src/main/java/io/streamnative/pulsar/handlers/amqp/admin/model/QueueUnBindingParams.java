package io.streamnative.pulsar.handlers.amqp.admin.model;

import lombok.Data;

@Data
public class QueueUnBindingParams {
    private String destination;
    private String destination_type;
    private String properties_key;
    private String source;
    private String vhost;
}
