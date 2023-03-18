/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streamnative.pulsar.handlers.amqp.utils;

import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.ARGUMENTS;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.AUTO_DELETE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.DURABLE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.PASSIVE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue.QUEUE;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class QueueUtil {

    public static final ObjectMapper JSON_MAPPER = new JsonMapper();

    public static Map<String, String> generateTopicProperties(String queueName, boolean durable, boolean autoDelete,
                                                              boolean passive, Map<String, Object> arguments)
            throws JsonProcessingException {
        if (StringUtils.isEmpty(queueName)) {
            throw new AoPServiceRuntimeException.ExchangeParameterException("Miss parameter queue name.");
        }
        Map<String, String> props = new HashMap<>(8);
        props.put(QUEUE, queueName);
        props.put(DURABLE, "" + durable);
        props.put(AUTO_DELETE, "" + autoDelete);
        props.put(PASSIVE, "" + passive);
        props.put(ARGUMENTS,
                covertObjectValueAsString(Objects.requireNonNullElseGet(arguments, () -> new HashMap<>(1))));
        return props;
    }

    public static String covertObjectValueAsString(Object obj) throws JsonProcessingException {
        return JSON_MAPPER.writeValueAsString(obj);
    }

    public static Map<String, Object> covertStringValueAsObjectMap(String value) throws JsonProcessingException {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return JSON_MAPPER.readValue(value, new TypeReference<>() {});
    }
}
