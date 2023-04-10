package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class QueueDeleteParams {

    @JsonProperty("if-unused")
    private boolean ifUnused;
    @JsonProperty("if-empty")
    private boolean ifEmpty;
    private String mode;
    private String name;
    private String vhost;
}
