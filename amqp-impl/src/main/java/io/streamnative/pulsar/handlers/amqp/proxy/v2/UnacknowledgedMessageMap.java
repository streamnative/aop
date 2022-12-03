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
package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

/**
 * unack message map.
 */
public class UnacknowledgedMessageMap {

    private final Map<Long, MessageConsumerAssociation> map = new ConcurrentHashMap<>();
    private final AmqpProxyServerChannel channel;

    public UnacknowledgedMessageMap(AmqpProxyServerChannel channel) {
        this.channel = channel;
    }

    /**
     * unAck positionInfo.
     */
    public static final class MessageConsumerAssociation {
        @Getter
        private final MessageId messageId;
        private final AmqpConsumer consumer;
        @Getter
        private final int size;

        private MessageConsumerAssociation(MessageId messageId, AmqpConsumer consumer, int size) {
            this.messageId = messageId;
            this.consumer = consumer;
            this.size = size;
        }

        public Consumer<byte[]> getConsumer() {
            return consumer.getConsumer();
        }

    }

    public Collection<MessageConsumerAssociation> acknowledge(long deliveryTag, boolean multiple) {
        if (multiple) {
            Map<Long, MessageConsumerAssociation> acks = new HashMap<>();
            map.entrySet().stream().forEach(entry -> {
                if (entry.getKey() <= deliveryTag) {
                    acks.put(entry.getKey(), entry.getValue());
                }
            });
            remove(acks.keySet());
            return acks.values();
        } else {
            MessageConsumerAssociation association = remove(deliveryTag);
            if (association != null) {
                return Collections.singleton(association);
            }
        }
        return Collections.emptySet();
    }

    public Collection<MessageConsumerAssociation> acknowledgeAll() {
        Set<MessageConsumerAssociation> associations = new HashSet<>();
        associations.addAll(map.values());
        remove(map.keySet());
        return associations;
    }

    public void add(long deliveryTag, MessageId messageId, AmqpConsumer consumer, int size) {
        checkNotNull(messageId);
        checkNotNull(consumer);
        map.put(deliveryTag, new MessageConsumerAssociation(messageId, consumer, size));
    }

    public void remove(Collection<Long> deliveryTag) {
        deliveryTag.stream().forEach(tag -> {
            MessageConsumerAssociation entry = map.remove(tag);
            if (entry != null) {
//                channel.restoreCredit(1, entry.getSize());
            }
        });
    }

    public MessageConsumerAssociation remove(long deliveryTag) {
        MessageConsumerAssociation entry = map.remove(deliveryTag);
        if (entry != null) {
//            channel.restoreCredit(1, entry.getSize());
        }
        return entry;
    }

    public int size() {
        return map.size();
    }

    @VisibleForTesting
    public Map<Long, MessageConsumerAssociation> getMap() {
        return map;
    }

}
