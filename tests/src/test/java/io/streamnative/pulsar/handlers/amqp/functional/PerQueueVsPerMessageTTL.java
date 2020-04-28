
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

package io.streamnative.pulsar.handlers.amqp.functional;

import static org.junit.Assert.assertNull;
import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class PerQueueVsPerMessageTTL extends PerMessageTTL {

    @Test
    public void smallerPerQueueExpiryWins() throws IOException, InterruptedException {
        declareAndBindQueue(10);
        this.sessionTTL = 1000;

        publish("message1");

        Thread.sleep(100);

        assertNull("per-queue ttl should have removed message after 10ms!", get());
    }

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        final Object mappedTTL = (ttlValue instanceof String &&
                ((String) ttlValue).contains("foobar")) ?
                ttlValue : longValue(ttlValue) * 2;
        this.sessionTTL = ttlValue;
        Map<String, Object> argMap = Collections.singletonMap(PerQueueTTL.TTL_ARG, mappedTTL);
        return this.channel.queueDeclare(name, false, true, false, argMap);
    }

    private Long longValue(final Object ttl) {
        if (ttl instanceof Short) {
            return ((Short) ttl).longValue();
        } else if (ttl instanceof Integer) {
            return ((Integer) ttl).longValue();
        } else if (ttl instanceof Long) {
            return (Long) ttl;
        } else {
            throw new IllegalArgumentException("ttl not of expected type");
        }
    }

}
