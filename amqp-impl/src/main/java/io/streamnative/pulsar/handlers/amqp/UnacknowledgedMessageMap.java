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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;


/**
 * unack message map.
 */
public class UnacknowledgedMessageMap {

    public interface MessageProcessor {
        void messageAck(Position position);
        void requeue(List<PositionImpl> positions);
        default void discardMessage(List<PositionImpl> positions){}
    }

    /**
     * unAck positionInfo.
     */
    public static final class MessageConsumerAssociation {
        private final Position position;
        private final MessageProcessor consumer;
        private final int size;

        private MessageConsumerAssociation(Position position, MessageProcessor consumer, int size) {
            this.position = position;
            this.consumer = consumer;
            this.size = size;
        }

        public Position getPosition() {
            return position;
        }

        public MessageProcessor getConsumer() {
            return consumer;
        }

        public int getSize() {
            return size;
        }
    }

    private final Map<Long, MessageConsumerAssociation> map = new ConcurrentHashMap<>();
    private final AmqpChannel channel;
    public UnacknowledgedMessageMap(AmqpChannel channel) {
        this.channel = channel;
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

    public void add(long deliveryTag, Position position, MessageProcessor consumer, int size) {
        checkNotNull(position);
        checkNotNull(consumer);
        map.put(deliveryTag, new MessageConsumerAssociation(position, consumer, size));
    }

    public void remove(Collection<Long> deliveryTag) {
        deliveryTag.stream().forEach(tag -> {
            MessageConsumerAssociation entry = map.remove(tag);
            if (entry != null) {
                channel.restoreCredit(1, entry.getSize());
            }
        });
    }

    public MessageConsumerAssociation remove(long deliveryTag) {
        MessageConsumerAssociation entry = map.remove(deliveryTag);
        if (entry != null) {
            channel.restoreCredit(1, entry.getSize());
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
