

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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * PerQueueTTL.
 */
public class PerQueueTTL extends TTLHandling {

    protected static final String TTL_ARG = "x-message-ttl";

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        Map<String, Object> argMap = Collections.singletonMap(TTL_ARG, ttlValue);
        return this.channel.queueDeclare(name, false, true, false, argMap);
    }

    //@Test
    public void queueReDeclareEquivalence() throws Exception {
        declareQueue(10);
        try {
            declareQueue(20);
            fail("Should not be able to redeclare with different x-message-ttl");
        } catch (IOException ex) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ex);
        }
    }

    //@Test
    public void queueReDeclareSemanticEquivalence() throws Exception {
        declareQueue((byte) 10);
        declareQueue(10);
        declareQueue((short) 10);
        declareQueue(10L);
    }

    //@Test
    public void queueReDeclareSemanticNonEquivalence() throws Exception {
        declareQueue(10);
        try {
            declareQueue(10.0);
            fail("Should not be able to redeclare with x-message-ttl argument of different type");
        } catch (IOException ex) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, ex);
        }
    }

    protected void publishWithExpiration(String msg, Object sessionTTL) throws IOException {
        basicPublishVolatile(msg.getBytes(), TTL_EXCHANGE, TTL_QUEUE_NAME,
                MessageProperties.TEXT_PLAIN
                        .builder()
                        .expiration(String.valueOf(sessionTTL))
                        .build());
    }
}
