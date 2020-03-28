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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.bookkeeper.mledger.Position;



/**
 * unack message map.
 */
public class UnacknowledgedMessageMap {

    /**
     *  unAck positionInfo.
     */
    public static final class MessageConsumerAssociation {
        private final Position position;
        private final AmqpConsumer consumer;

        private MessageConsumerAssociation(Position position, AmqpConsumer consumer) {
            this.position = position;
            this.consumer = consumer;
        }

        public Position getPosition() {
            return position;
        }

        public AmqpConsumer getConsumer() {
            return consumer;
        }
    }

    private final Map<Long, MessageConsumerAssociation> map = new ConcurrentHashMap<>();

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
            MessageConsumerAssociation association = map.remove(deliveryTag);
            if (association != null) {
                return Collections.singleton(association);
            }
        }
        return Collections.emptySet();
    }

    public void add(long deliveryTag, Position position, AmqpConsumer consumer) {
        checkNotNull(position);
        checkNotNull(consumer);
        map.put(deliveryTag, new UnacknowledgedMessageMap.MessageConsumerAssociation(position, consumer));
    }

    public void remove(Collection<Long> deliveryTag) {
        deliveryTag.stream().forEach(tag -> {
            map.remove(tag);
        });
    }

    public int size() {
        return map.size();
    }

    @VisibleForTesting
    public Map<Long, MessageConsumerAssociation> getMap() {
        return map;
    }
}
