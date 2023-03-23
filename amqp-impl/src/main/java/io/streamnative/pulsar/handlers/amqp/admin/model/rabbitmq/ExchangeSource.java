package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ExchangeSource {

    private String source;
    private String vhost;
    private String destination;
    private String destination_type;
    private String routing_key;
    private Map<String, Object> arguments;
    private String properties_key;

}
