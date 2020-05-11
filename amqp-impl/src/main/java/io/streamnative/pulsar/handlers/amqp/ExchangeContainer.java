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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;

/**
 * Container for all exchanges in the broker.
 */
public class ExchangeContainer {

    private static Map<String, AmqpExchange> exchangeMap = new ConcurrentHashMap<>();

    public static void putExchange(String exchangeName, AmqpExchange amqpExchange) {
        exchangeMap.computeIfAbsent(exchangeName, name -> amqpExchange);
    }

    public static AmqpExchange getExchange(String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return null;
        }
        return exchangeMap.getOrDefault(exchangeName, null);
    }

    public static void deleteExchange(String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        exchangeMap.remove(exchangeName);
    }
}
