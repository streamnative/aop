package io.streamnative.pulsar.handlers.amqp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AmqpBinding {

    private String source;
    private String bindingKey;
    private Map<String, Object> params;

}
