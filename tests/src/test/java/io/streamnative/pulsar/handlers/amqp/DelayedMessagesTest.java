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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Admin API test.
 */
@Slf4j
public class DelayedMessagesTest extends AmqpTestBase{

    @Test()
    public void massiveDelayTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();

        String ex = randExName();
        String qu = randQuName();

        Map<String, Object> map = new HashMap<>();
        map.put("x-delayed-type", "fanout");
        channel.exchangeDeclare(ex, "x-delayed-message", true, false, map);
        channel.queueDeclare(qu, true, false, false, null);
        channel.queueBind(qu, ex, "");

        Set<String> messageSet = new HashSet<>();
        long st = System.currentTimeMillis();

        for (int i = 0; i < 1000; i++) {
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);

            AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();
            Map<String, Object> headers = new HashMap<>();
            headers.put("x-delay", 1000 * 5);
            props.headers(headers);
            String msg = "test-" + i;
            messageSet.add(msg);
            channel.basicPublish(ex, "", props.build(), msg.getBytes());
        }
        System.out.println("send all messages in " + (System.currentTimeMillis() - st) / 1000 + "s");

        Thread.sleep(1000 * 5);

        channel.basicQos(1);
        channel.basicConsume(qu, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String msg = new String(body);
                messageSet.remove(msg);
                System.out.println("receive message after " + (System.currentTimeMillis() - st) / 1000 + " s");
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        Awaitility.await().pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .until(messageSet::isEmpty);
        System.out.println("receive all messages in " + (System.currentTimeMillis() - st) / 1000 + "s");
    }

}
