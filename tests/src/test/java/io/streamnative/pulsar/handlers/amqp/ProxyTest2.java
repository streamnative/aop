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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * AMQP proxy related test.
 */
@Slf4j
public class ProxyTest2 extends AmqpTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        setBrokerCount(3);
        super.setup();
    }

    @Test
    public void rabbitMQProxyTest() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setVirtualHost("vhost1");
        factory.setPort(getProxyPort());
        Connection coon = factory.newConnection();
        Channel channel = coon.createChannel();

        System.out.println("xxxx [Client] start exchange declare");
        String ex = "exchange";
        Map<String, Object> map = new HashMap<>();
        map.put("key-a", "a value");
        map.put("key-b", "b value");
        channel.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true, false, map);
        System.out.println("xxxx [Client] finish exchange declare");

        channel.close();
        coon.close();
    }

}
