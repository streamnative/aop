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
import com.rabbitmq.client.DefaultConsumer;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

public class RabbitMQAminDemo {

    @Test
    public void test2() throws Exception {
        Connection connection = getConnection("vhost1", 5672);
        Channel channel = connection.createChannel();
        channel.exchangeDeclare("ex1-1", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-2", BuiltinExchangeType.DIRECT, true);
        channel.exchangeDeclare("ex1-3", BuiltinExchangeType.DIRECT, true);

        channel.queueDeclare("qu1-1", true, false, false, null);
        channel.queueDeclare("qu1-2", true, false, false, null);
        channel.queueDeclare("qu1-3", true, false, false, null);

        Connection connection2 = getConnection("vhost2", 5672);
        Channel channel2 = connection2.createChannel();
        channel2.exchangeDeclare("ex2-1", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-2", BuiltinExchangeType.DIRECT, true);
        channel2.exchangeDeclare("ex2-3", BuiltinExchangeType.DIRECT, true);

        channel2.queueDeclare("qu1-1", true, false, false, null);
        channel2.queueDeclare("qu1-2", true, false, false, null);
        channel2.queueDeclare("qu1-3", true, false, false, null);

        channel.close();
        connection.close();
    }

    @Test
    public void test3() throws Exception {
        String queue = "qu1-2";

        Connection connection = getConnection("vhost1", 5672);
        Channel channel = connection.createChannel();
        channel.queueDelete(queue);
        channel.queueDeclare(queue, true, false, true, null);
        channel.queueBind(queue, "ex1-1", "");
//        channel.queueDelete(queue);

        Connection connection2 = getConnection("vhost1", 5672);
        Channel channel2 = connection2.createChannel();
        channel2.queueDeclare(queue, true, false, true, null);
    }

    @Test
    public void test4() throws Exception {
        String queue = "qu1-4";

        Connection connection = getConnection("vhost1", 5672);
        Channel channel = connection.createChannel();
        channel.queueDeclare(queue, true, true, false, null);
        channel.queueBind(queue, "ex1-1", "");
//        channel.queueDelete(queue);

        Connection connection2 = getConnection("vhost1", 5672);
        Channel channel2 = connection2.createChannel();
//        channel2.queueBind(queue, "ex1-1", "");
//        channel2.queueUnbind(queue, "ex1-1", "");
//        channel2.queueDeclare(queue, true, false, false, null);
//        channel2.queuePurge(queue);
        channel2.basicPublish("ex1-1", queue, null, "test".getBytes());
        channel2.basicConsume(queue, new DefaultConsumer(channel2) {

        });
//        channel2.queueDelete(queue);
    }

    protected Connection getConnection(String vhost, int port) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost(vhost);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        return connectionFactory.newConnection();
    }

}
