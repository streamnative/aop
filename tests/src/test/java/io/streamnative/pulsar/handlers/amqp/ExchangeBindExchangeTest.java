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
import com.rabbitmq.client.BuiltinExchangeType;
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
import lombok.Cleanup;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Exchange bind exchange test.
 */
public class ExchangeBindExchangeTest extends AmqpTestBase {

    @Test
    public void bindAndUnbindTest() throws Exception {
        @Cleanup
        Connection connection = getConnection("vhost1", true);
        @Cleanup
        Channel channel = connection.createChannel();

        String sourceEx = randExName();
        String desEx = randExName();
        channel.exchangeDeclare(sourceEx, BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare(desEx, BuiltinExchangeType.DIRECT, true);
        String routingKey = "ex-routing";
        Map<String, Object> params = new HashMap<>();
        params.put("int", 100);
        params.put("string", "string value");
        params.put("boolean", true);
        channel.exchangeBind(desEx, sourceEx, routingKey, params);
        channel.exchangeUnbind(desEx, sourceEx, routingKey, params);
    }

    /**
     * A demo case test, data pipeline as below.
     *
     * TOPIC_EX --> HEADER_EX  --> TOPIC_EX --> QUEUE1
     *                        |--> DIRECT_EX --> QUEUE1
     *                        |--> FANOUT_EX --> QUEUE2
     */
    @Test(timeOut = 1000 * 5)
    public void produceAndConsumeTest() throws Exception {
        @Cleanup
        Connection connection = getConnection("vhost1", true);
        @Cleanup
        Channel channel = connection.createChannel();
        String rootEx, firstLevelEx, secondLevelEx1, secondLevelEx2, secondLevelEx3;
        rootEx = "root-ex-" + RandomStringUtils.randomAlphabetic(5);
        firstLevelEx = "level1-ex-header-" + RandomStringUtils.randomAlphabetic(5);
        secondLevelEx1 = "level2-ex-topic-" + RandomStringUtils.randomAlphabetic(5);
        secondLevelEx2 = "level2-ex-fanout-" + RandomStringUtils.randomAlphabetic(5);
        secondLevelEx3 = "level2-ex-direct-" + RandomStringUtils.randomAlphabetic(5);
        channel.exchangeDeclare(rootEx, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(firstLevelEx, BuiltinExchangeType.HEADERS, true);
        channel.exchangeDeclare(secondLevelEx1, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(secondLevelEx2, BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare(secondLevelEx3, BuiltinExchangeType.FANOUT, true);

        channel.exchangeBind(firstLevelEx, rootEx, "#");

        Map<String, Object> map1 = new HashMap<>();
        map1.put("second-ex1", true);
        channel.exchangeBind(secondLevelEx1, firstLevelEx, "", map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("second-ex2", true);
        channel.exchangeBind(secondLevelEx2, firstLevelEx, "", map2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("second-ex3", true);
        channel.exchangeBind(secondLevelEx3, firstLevelEx, "", map3);

        Set<String> messageSet = new HashSet<>();

        String queue1 = "queue1-" + RandomStringUtils.randomAlphabetic(5);
        channel.queueDeclare(queue1, true, false, false, null);
        final String keyLevel2Ex1ToQueue1 = "Level1Ex1.queue1.*";
        channel.queueBind(queue1, secondLevelEx1, keyLevel2Ex1ToQueue1);
        final String keyLevel2Ex2ToQueue2 = "Level2Ex2.queue2";
        channel.queueBind(queue1, secondLevelEx2, keyLevel2Ex2ToQueue2);
        channel.basicConsume(queue1, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                System.out.println("[" + queue1 + "] receive msg: " + new String(body));
                messageSet.remove(new String(body));
            }
        });

        String queue2 = "queue2-" + RandomStringUtils.randomAlphabetic(5);
        channel.queueDeclare(queue2, true, false, false, null);
        channel.queueBind(queue2, secondLevelEx3, "");
        channel.basicConsume(queue2, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                System.out.println("[" + queue2 + "] receive msg: " + new String(body));
                messageSet.remove(new String(body));
            }
        });

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            basicProperties.headers(map1);
            String key = keyLevel2Ex1ToQueue1.replace("*", "x");
            String msg = "[" + i + "] with key " + key;
            messageSet.add(msg);
            channel.basicPublish(rootEx, key, basicProperties.build(), msg.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            basicProperties.headers(map2);
            String msg = "[" + i + "] with key " + keyLevel2Ex2ToQueue2;
            messageSet.add(msg);
            channel.basicPublish(rootEx, keyLevel2Ex2ToQueue2, basicProperties.build(), msg.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            basicProperties.headers(map3);
            String msg = "[" + i + "] with key empty";
            messageSet.add(msg);
            channel.basicPublish(rootEx, "", basicProperties.build(), msg.getBytes());
        }

        System.out.println("finish publish messages");
        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.SECONDS).until(messageSet::isEmpty);
    }

}
