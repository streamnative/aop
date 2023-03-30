package io.streamnative.pulsar.handlers.amqp.admin.model;

import lombok.Data;

@Data
public class MessageParams {
    private String messageId;
    private String encoding;
    private String startTime;
    private String endTime;
    private String name;
    private String vhost;
    private int messages;
}
