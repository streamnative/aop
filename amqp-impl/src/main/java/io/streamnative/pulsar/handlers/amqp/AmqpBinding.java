package io.streamnative.pulsar.handlers.amqp;

import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AmqpBinding {

    private String source;
    private String bindingKey;
    private Map<String, Object> params;

    public String getPropsKey() {
        if (params == null || params.isEmpty()) {
            return bindingKey;
        } else {
            return bindingKey + "~" + params.hashCode();
        }
    }

}
