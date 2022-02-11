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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;

/**
 * Amqp consumer Used to return pull messages.
 */
@Slf4j
public class AmqpPullConsumer extends AmqpConsumer {

    public AmqpPullConsumer(QueueContainer queueContainer, Subscription subscription,
        CommandSubscribe.SubType subType, String topicName, long consumerId, int priorityLevel,
        String consumerName, boolean isDurable, ServerCnx cnx, String appId,
        Map<String, String> metadata, boolean readCompacted,
        CommandSubscribe.InitialPosition subscriptionInitialPosition,
        KeySharedMeta keySharedMeta, AmqpChannel channel, String consumerTag, String queueName,
        boolean autoAck) throws BrokerServiceException {
        super(queueContainer, subscription, subType, topicName, consumerId, priorityLevel, consumerName,
                isDurable, cnx, appId, metadata, readCompacted, subscriptionInitialPosition, keySharedMeta, channel,
            consumerTag, queueName, autoAck);
    }

    @Override
    public int getAvailablePermits() {
        return 0;
    }

    @Override public boolean isBlocked() {
        return true;
    }
}
