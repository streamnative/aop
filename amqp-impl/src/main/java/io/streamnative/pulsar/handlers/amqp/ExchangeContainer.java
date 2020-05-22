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

package io.streamnative.pulsar.handlers.amqp;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Container for all exchanges in the broker.
 */
@Slf4j
public class ExchangeContainer {

    @Getter
    private static Map<NamespaceName, Map<String, AmqpExchange>> exchangeMap = new ConcurrentHashMap<>();

    public static void putExchange(NamespaceName namespaceName, String exchangeName, AmqpExchange amqpExchange) {
        exchangeMap.compute(namespaceName, (name, map) -> {
            Map<String, AmqpExchange> amqpExchangeMap = map;
            if (amqpExchangeMap == null) {
                amqpExchangeMap = Maps.newConcurrentMap();
            }
            amqpExchangeMap.put(exchangeName, amqpExchange);
            return amqpExchangeMap;
        });
    }

    public static AmqpExchange getExchange(NamespaceName namespaceName, String exchangeName) {
        if (namespaceName == null || StringUtils.isEmpty(exchangeName)) {
            return null;
        }
        Map<String, AmqpExchange> map = exchangeMap.getOrDefault(namespaceName, null);
        if (map == null) {
            return null;
        }
        return map.getOrDefault(exchangeName, null);
    }

    public static void deleteExchange(NamespaceName namespaceName, String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        if (exchangeMap.containsKey(namespaceName)) {
            exchangeMap.get(namespaceName).remove(exchangeName);
        }
    }

}
