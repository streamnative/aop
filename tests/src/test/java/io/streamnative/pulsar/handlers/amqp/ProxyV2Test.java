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

import static org.testng.AssertJUnit.assertFalse;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * AMQP proxy related test.
 */
@Slf4j
public class ProxyV2Test extends AmqpTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        setBrokerCount(3);
        this.conf.setAmqpMultiBundleEnable(true);
        this.conf.setDefaultNumberOfNamespaceBundles(16);
        super.setup();
    }

    @Test
    public void e2eTest() throws Exception {
        int port = getAmqpBrokerPortList().get(0);
        @Cleanup
        Connection conn = getConnection("vhost1", port);
        @Cleanup
        Channel channel = conn.createChannel();

        String ex = randExName();
        channel.exchangeDeclare(ex, "direct", false, true, null);

        String qu1 = randQuName();
        channel.queueDeclare(qu1, false, false, true, null);
        String key1 = "key1";
        channel.queueBind(qu1, ex, key1);

        String qu2 = randQuName();
        channel.queueDeclare(qu2, false, false, true, null);
        String key2 = "key2";
        channel.queueBind(qu2, ex, key2);

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            channel.basicPublish(ex, key1, null, (key1 + "-" + i).getBytes());
            channel.basicPublish(ex, key2, null, (key2 + "-" + i).getBytes());
        }

        AtomicInteger receiveCount = new AtomicInteger();
        AtomicBoolean flag1 = new AtomicBoolean(false);
        channel.basicConsume(qu1, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                receiveCount.incrementAndGet();
                if (!new String(body).contains(key1)) {
                    flag1.set(true);
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        AtomicBoolean flag2 = new AtomicBoolean(false);
        channel.basicConsume(qu2, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                receiveCount.incrementAndGet();
                if (!new String(body).contains(key2)) {
                    flag2.set(true);
                }
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
                .until(() -> receiveCount.get() == messageCount * 2);
        assertFalse(flag1.get());
        assertFalse(flag2.get());
        channel.queueUnbind(qu1, ex, key1);
        channel.queueUnbind(qu2, ex, key2);
    }

}
