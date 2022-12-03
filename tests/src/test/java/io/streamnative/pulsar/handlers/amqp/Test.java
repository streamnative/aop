package io.streamnative.pulsar.handlers.amqp;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Test {

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setVirtualHost("/");
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(5682);
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        String ex = "ex-perf";
        String qu = "qu-perf";
        channel.exchangeDeclare(ex, BuiltinExchangeType.DIRECT, true);
        channel.queueDeclare(qu, true, false, false, null);
        channel.queueBind(qu, ex, qu);
        channel.close();
        connection.close();
        System.out.println("init completely");
    }

}
