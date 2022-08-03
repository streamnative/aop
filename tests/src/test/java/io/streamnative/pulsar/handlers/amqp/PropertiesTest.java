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
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Admin API test.
 */
@Slf4j
public class PropertiesTest extends AmqpTestBase{

    @Test()
    public void listExchangeTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();

        String ex = randExName();
        String qu = randQuName();


        channel.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(qu, true, false, false, null);
        channel.queueBind(qu, ex, "");


        channel.basicConsume(qu, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
            }
        });

        AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();
        Map<String, Object> headers = new HashMap<>();
        headers.put("a", "av");
        headers.put("b", "bv");
        props.headers(headers);
        channel.basicPublish(ex, "", props.build(), "test".getBytes());

        Thread.sleep(1000 * 60 * 60);
    }

    interface ReadCallback {
        void readFailed(String s);
    }

    public static void main(String[] args) throws InterruptedException {
        ExecutorService executors = Executors.newSingleThreadExecutor();

        ReadCallback readCallback = new ReadCallback() {
            @Override
            public void readFailed(String s) {
                log.info(s.toLowerCase());
            }
        };

        PropertiesTest test = new PropertiesTest();
        executors.submit(() -> {
            test.read(executors, readCallback);
        });
        System.out.println("test finish");
        Thread.sleep(1000 * 2);
        System.out.println("test finish 2");
        executors.shutdown();
    }

    public void read(ExecutorService executorService, ReadCallback readCallback) {
        try {
            log.info("read method");
            read0(executorService, readCallback);
        } catch (Throwable e) {
            log.error("read 0 error ", e);
        }
    }

    public void read0(ExecutorService executorService, ReadCallback readCallback) {
        process().thenAcceptAsync(s -> {
            log.info("process finish");
            readCallback.readFailed(s);
        }, executorService).exceptionally(s -> {
            log.error("failed to process", s);
            return null;
        });
    }

    public CompletableFuture<String> process() {
        return CompletableFuture.completedFuture(null);
    }

}
