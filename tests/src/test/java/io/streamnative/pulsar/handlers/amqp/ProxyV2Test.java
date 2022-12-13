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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
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
        ((AmqpServiceConfiguration) this.conf).setAmqpMultiBundleEnable(true);
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
        String qu = randQuName();
        channel.queueDeclare(qu, false, false, true, null);
        String routingKey = "key1";
        channel.queueBind(qu, ex, routingKey);

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            channel.basicPublish(ex, routingKey, null, "data".getBytes());
        }

        AtomicInteger receiveCount = new AtomicInteger();
        channel.basicConsume(qu, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("receive message " + new String(body));
                receiveCount.incrementAndGet();
            }
        });
        Awaitility.waitAtMost(5, TimeUnit.SECONDS)
                .until(() -> receiveCount.get() == messageCount);
    }

}
