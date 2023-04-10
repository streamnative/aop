package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ExchangeDeleteParams {

    @JsonProperty("if-unused")
    private boolean ifUnused;
    private String name;
    private String vhost;
}
