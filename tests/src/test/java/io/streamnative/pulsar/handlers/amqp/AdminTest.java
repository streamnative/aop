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
import org.testng.annotations.Test;

public class AdminTest extends AmqpTestBase{

    @Test
    public void test() throws Exception {
        Connection connection = getConnection("vhost1", false);
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("ex1-1", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-2", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-3", BuiltinExchangeType.DIRECT, true);

        Connection connection2 = getConnection("vhost2", false);
        Channel channel2 = connection2.createChannel();
        channel2.exchangeDeclare("ex2-1", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-2", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-3", BuiltinExchangeType.DIRECT, true);

        System.out.println("aop server start");
        Thread.sleep(1000 * 60 * 60);
    }

    @Test
    public void test2() throws Exception {
        Connection connection = getConnection("vhost1", 5672);
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("ex1-1", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-2", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-3", BuiltinExchangeType.DIRECT, true);

        Connection connection2 = getConnection("vhost2", 5672);
        Channel channel2 = connection2.createChannel();
        channel2.exchangeDeclare("ex2-1", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-2", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-3", BuiltinExchangeType.DIRECT, true);
    }

}
