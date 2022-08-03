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
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

        long st2 = System.currentTimeMillis();
        for (int i = 0; i < 50000; i++) {
            AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();
            Map<String, Object> headers = new HashMap<>();
            headers.put("x-delay", 1000 * 5);
            props.headers(headers);
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);
            mockBookKeeper.addEntryDelay(0, TimeUnit.SECONDS);
            String msg = "test-" + i;
            messageSet.add(msg);
            channel.basicPublish(ex, "", props.build(), msg.getBytes());
        }
        System.out.println("send all messages in " + (System.currentTimeMillis() - st2) / 1000 + "s");

        Thread.sleep(1000 * 5);

        channel.basicQos(1);
        channel.basicConsume(qu, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body);
                messageSet.remove(msg);
//                System.out.println("receive message after " + (System.currentTimeMillis() - st) / 1000 + " s");
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        Awaitility.await().pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .until(messageSet::isEmpty);
        System.out.println("receive all messages in " + (System.currentTimeMillis() - st2) / 1000 + "s");
//        Thread.sleep(1000 * 60 * 60);
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

        DelayedMessagesTest test = new DelayedMessagesTest();
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
