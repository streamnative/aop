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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Properties test.
 */
@Slf4j
public class PropertiesTest extends AmqpTestBase{

    @Test(timeOut = 5000)
    public void propsTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();

        String ex = randExName();
        String qu = randQuName();

        channel.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(qu, true, false, false, null);
        channel.queueBind(qu, ex, "");

        Map<String, Object> headers = new HashMap<>();
        headers.put("int", 10);
        headers.put("double", 20.123);
        headers.put("boolean", true);

        AtomicBoolean flag = new AtomicBoolean(false);
        channel.basicConsume(qu, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                if (properties.getHeaders().equals(headers)) {
                    flag.set(true);
                }
            }
        });

        AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();
        props.headers(headers);
        channel.basicPublish(ex, qu, props.build(), "test".getBytes());

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(flag::get);
    }

}
