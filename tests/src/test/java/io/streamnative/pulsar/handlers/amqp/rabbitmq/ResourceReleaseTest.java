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
package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;

/**
 * Resource release test.
 */
@Slf4j
public class ResourceReleaseTest extends RabbitMQTestBase {

    //@Test
    public void bundleUnloadTest() throws IOException, TimeoutException, InterruptedException {

        @Cleanup
        final Connection connection = getConnection("vhost1", false);
        @Cleanup
        Channel channel = connection.createChannel();

        String msgContent = "Hello AOP.";
        String exchange = randExName();
        String queue = randQuName();
        channel.exchangeDeclare(exchange, BuiltinExchangeType.FANOUT, true);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, "");

        final int msgCnt = 100;
        AtomicBoolean couldSendMsg = new AtomicBoolean(true);
        AtomicInteger consumeCnt = new AtomicInteger(0);
        new Thread(() -> {
                AtomicInteger sendMsgCnt = new AtomicInteger(0);
                while (consumeCnt.get() < msgCnt) {
                    try {
                        if (couldSendMsg.get()) {
                            channel.basicPublish(exchange, "", null, msgContent.getBytes());
                        }
                        if (sendMsgCnt.get() == 10) {
                            couldSendMsg.set(false);
                        }
                        Thread.sleep(10);
                    } catch (Exception e) {
                    }
                }
        }).start();

        CountDownLatch consumeLatch = new CountDownLatch(msgCnt);
        try {
            Channel consumeChannel = connection.createChannel();
            Consumer consumer = new DefaultConsumer(consumeChannel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    synchronized (consumeLatch) {
                        consumeLatch.countDown();
                    }
                    consumeCnt.addAndGet(1);
                    log.info("Receive msg [{}]", consumeCnt.get());
                    Assert.assertEquals(msgContent, new String(body));
                }
            };
            consumeChannel.basicConsume(queue, consumer);
        } catch (Exception e) {
            log.error("Failed to consume message.", e);
        }

        try {
            admin.namespaces().unload("public/vhost1");
            log.info("unload namespace public/vhost1.");
        } catch (Exception e) {
            Assert.fail("Unload namespace public/vhost1 failed. errorMsg: " + e.getMessage());
        }

        couldSendMsg.set(true);
        consumeLatch.await();

        log.info("bundleUnloadTest finish.");
    }

}
