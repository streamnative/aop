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

import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Container for all queues in the broker.
 */
public class QueueContainer extends TopicContainer {

    @Getter
    private final static Map<NamespaceName, Map<String, AmqpQueue>> queueMap = new ConcurrentHashMap<>();

    public static void putQueue(NamespaceName namespaceName, String queueName, AmqpQueue amqpQueue) {
        queueMap.compute(namespaceName, (ns, map) -> {
            Map<String, AmqpQueue> amqpQueueMap = map;
            if (amqpQueueMap == null) {
                amqpQueueMap = Maps.newConcurrentMap();
            }
            amqpQueueMap.put(queueName, amqpQueue);
            return amqpQueueMap;
        });
    }

    public static AmqpQueue getQueue(NamespaceName namespaceName, String queueName) {
        if (namespaceName == null || StringUtils.isEmpty(queueName)) {
            return null;
        }
        Map<String, AmqpQueue> map = queueMap.getOrDefault(namespaceName, null);
        if (map == null) {
            return null;
        }
        return map.getOrDefault(queueName, null);
    }

    public static void deleteQueue(String exchangeName) {
        if (StringUtils.isEmpty(exchangeName)) {
            return;
        }
        queueMap.remove(exchangeName);
    }
}
