package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PublishParams {

    @JsonProperty("delivery_mode")
    private String deliveryMode;
    @JsonProperty("routing_key")
    private String routingKey;
    @JsonProperty("payload_encoding")
    private String payloadEncoding;
    private String name;
    private String payload;
    private String vhost;
    private Map<String, Object> headers;
    private Map<String, Object> properties;
    private Map<String, String> props;
}
