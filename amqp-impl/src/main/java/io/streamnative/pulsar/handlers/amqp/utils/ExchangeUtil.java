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

import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.ARGUMENTS;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.AUTO_DELETE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.DURABLE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.EXCHANGE;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.INTERNAL;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.QUEUES;
import static io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange.TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.streamnative.pulsar.handlers.amqp.common.exception.ExchangeParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

@Slf4j
public class ExchangeUtil {

    public static final ObjectMapper JSON_MAPPER = new JsonMapper();

    public static boolean isBuildInExchange(final String exchangeName) {
        return exchangeName.equals(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
                || (exchangeName.equals(ExchangeDefaults.FANOUT_EXCHANGE_NAME))
                || (exchangeName.equals(ExchangeDefaults.TOPIC_EXCHANGE_NAME));
    }

    public static String getExchangeType(String exchangeName) {
        String ex;
        if (exchangeName == null) {
            ex = "";
        } else {
            ex = exchangeName;
        }
        switch (ex) {
            case "":
            case ExchangeDefaults.DIRECT_EXCHANGE_NAME:
                return ExchangeDefaults.DIRECT_EXCHANGE_CLASS;
            case ExchangeDefaults.FANOUT_EXCHANGE_NAME:
                return ExchangeDefaults.FANOUT_EXCHANGE_CLASS;
            case ExchangeDefaults.TOPIC_EXCHANGE_NAME:
                return ExchangeDefaults.TOPIC_EXCHANGE_CLASS;
            default:
                return "";
        }
    }

    public static String formatExchangeName(String s) {
        return s.replaceAll("\r", "").
                replaceAll("\n", "").trim();
    }

    public static boolean isDefaultExchange(final String exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.toString().equals(exchangeName);
    }

    public static String covertObjectValueAsString(Object obj) {
        try {
            return JSON_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            log.error("Failed to covert object: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> covertStringValueAsObjectMap(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return JSON_MAPPER.readValue(value, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            log.error("Failed to covert string: {}", e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public static Map<String, String> generateTopicProperties(String exchangeName,
                                                              String exchangeType,
                                                              boolean durable,
                                                              boolean autoDelete,
                                                              boolean internal,
                                                              Map<String, Object> arguments,
                                                              List<String> queues) throws JsonProcessingException {
        if (StringUtils.isEmpty(exchangeName)) {
            throw new ExchangeParameterException("Miss parameter exchange name.");
        }
        if (StringUtils.isEmpty(exchangeType)) {
            throw new ExchangeParameterException("Miss parameter exchange type.");
        }
        Map<String, String> props = new HashMap<>();
        props.put(EXCHANGE, exchangeName);
        props.put(TYPE, exchangeType);
        props.put(DURABLE, "" + durable);
        props.put(AUTO_DELETE, "" + autoDelete);
        props.put(INTERNAL, "" + internal);
        if (arguments != null && !arguments.isEmpty()) {
            props.put(ARGUMENTS, covertObjectValueAsString(arguments));
        }
        if (!CollectionUtils.isEmpty(queues)) {
            props.put(QUEUES, JSON_MAPPER.writeValueAsString(queues));
        }
        return props;
    }

}
