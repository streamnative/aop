


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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.test.QueueingConsumer;
import java.io.IOException;
import org.junit.Test;

public class PerMessageTTL extends TTLHandling {

    protected Object sessionTTL;

    @Override
    protected void publish(String msg) throws IOException {
        basicPublishVolatile(msg.getBytes(), TTL_EXCHANGE, TTL_QUEUE_NAME,
                MessageProperties.TEXT_PLAIN
                        .builder()
                        .expiration(String.valueOf(sessionTTL))
                        .build());
    }

    @Override
    protected AMQP.Queue.DeclareOk declareQueue(String name, Object ttlValue) throws IOException {
        this.sessionTTL = ttlValue;
        return this.channel.queueDeclare(name, false, true, false, null);
    }

    @Test
    public void expiryWhenConsumerIsLateToTheParty() throws Exception {
        declareAndBindQueue(500);

        publish(MSG[0]);
        this.sessionTTL = 100;
        publish(MSG[1]);

        Thread.sleep(200);

        QueueingConsumer c = new QueueingConsumer(channel);
        channel.basicConsume(TTL_QUEUE_NAME, c);

        assertNotNull("Message unexpectedly expired", c.nextDelivery(100));
        assertNull("Message should have been expired!!", c.nextDelivery(100));
    }

    @Test
    public void restartingExpiry() throws Exception {
        final String expiryDelay = "2000";
        declareDurableQueue(TTL_QUEUE_NAME);
        bindQueue();
        channel.basicPublish(TTL_EXCHANGE, TTL_QUEUE_NAME,
                MessageProperties.MINIMAL_PERSISTENT_BASIC
                        .builder()
                        .expiration(expiryDelay)
                        .build(), new byte[]{});
        long expiryStartTime = System.currentTimeMillis();
        restart();
        Thread.sleep(Integer.parseInt(expiryDelay));
        try {
            assertNull("Message should have expired after broker restart", get());
        } finally {
            deleteQueue(TTL_QUEUE_NAME);
        }
    }

}
