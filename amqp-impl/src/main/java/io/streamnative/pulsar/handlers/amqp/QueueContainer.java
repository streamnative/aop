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
 * Container for all queues in the broker.
 */
public class QueueContainer {

    private static Map<String, AmqpQueue> queueMap = new ConcurrentHashMap<>();

    public static void putQueue(String namespaceName, String queueName, AmqpQueue amqpQueue) {
        queueMap.computeIfAbsent(generateKey(namespaceName, queueName), name -> amqpQueue);
    }

    public static AmqpQueue getQueue(String namespaceName, String queueName) {
        if (StringUtils.isEmpty(generateKey(namespaceName, queueName))) {
            return null;
        }
        return queueMap.getOrDefault(generateKey(namespaceName, queueName), null);
    }

    public static void deleteQueue(String namespaceName, String queueName) {
        if (StringUtils.isEmpty(generateKey(namespaceName, queueName))) {
            return;
        }
        queueMap.remove(generateKey(namespaceName, queueName));
    }

    private static String generateKey(String namespaceName, String exchangeName) {
        return namespaceName + "/" + exchangeName;
    }
}
