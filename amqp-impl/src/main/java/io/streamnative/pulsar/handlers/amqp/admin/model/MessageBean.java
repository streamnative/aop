package io.streamnative.pulsar.handlers.amqp.admin.model;

import java.util.Map;
import lombok.Data;

@Data
public class MessageBean {

    private String exchange;
    private long message_count;
    private String payload;
    private long payload_bytes;
    private String payload_encoding;
    private Map<String, Object> properties;
    private boolean redelivered;
    private String routing_key;
}
