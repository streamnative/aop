package io.streamnative.pulsar.handlers.amqp.admin.model;

import lombok.Data;

@Data
public class PurgeQueueParams {

    private String mode;
    private String name;
    private String vhost;
}
