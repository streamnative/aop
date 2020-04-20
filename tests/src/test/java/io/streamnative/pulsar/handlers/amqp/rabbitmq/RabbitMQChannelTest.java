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
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Return;
import com.rabbitmq.client.ReturnCallback;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.RpcClient;
import com.rabbitmq.client.RpcClientParams;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.UnroutableRpcRequestException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * RabbitMQ channel method tests.
 */
@Slf4j
public class RabbitMQChannelTest extends RabbitMQTestBase {

    private static final int timeout = 30000;

    @Test(timeOut = timeout)
    public void close_closes_channel() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.close();
                Assert.assertFalse(channel.isOpen());
            }
        }
    }

    @Test(timeOut = timeout)
    public void abort_closes_channel() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.abort();
                Assert.assertFalse(channel.isOpen());
            }
        }
    }

    @Test(timeOut = timeout)
    public void channel_number_can_be_accessed() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            int channelNumber = new Random().nextInt();
            try (Channel channel = conn.createChannel(channelNumber)) {
                Assert.assertEquals(channel.getChannelNumber(), channelNumber);
            }
        }
    }

    @Test(timeOut = timeout)
    public void getConnection_returns_the_actual_connection_which_created_the_channel() throws IOException,
            TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertEquals(channel.getConnection(), conn);
            }
        }
    }

    @Test(timeOut = timeout)
    public void basicQos_does_not_throw() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.basicQos(30);
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeDeclare_creates_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("test1", "fanout"));
                Assert.assertNotNull(channel.exchangeDeclarePassive("test1"));
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeDeclare_twice_keeps_existing_bindings() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String exchangeName = "test1";
                channel.exchangeDeclare(exchangeName, "fanout");
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, "unused");
                // Declare the same exchange a second time
                channel.exchangeDeclare(exchangeName, "fanout");

                channel.basicPublish("test1", "unused", null, "test".getBytes());
                GetResponse getResponse = channel.basicGet(queueName, true);

                Assert.assertNotNull(getResponse);
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = ShutdownSignalException.class)
    public void exchangeDeclare_twice_with_a_different_type_throws() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String exchangeName = "test1";
                channel.exchangeDeclare(exchangeName, "fanout");
                String queueName = channel.queueDeclare().getQueue();
                channel.queueBind(queueName, exchangeName, "unused");
                channel.exchangeDeclare(exchangeName, "topic");
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeDeclareNoWait_creates_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true,
                        false, false, null);
                Assert.assertNotNull(channel.exchangeDeclarePassive("test1"));
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = ShutdownSignalException.class)
    public void exchangeDeclarePassive_throws_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclarePassive("test1");
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IOException.class)
    public void exchangeDelete_removes_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true,
                        false, false, null);
                Assert.assertNotNull(channel.exchangeDelete("test1"));
                channel.exchangeDeclarePassive("test1");
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IOException.class)
    public void exchangeDeleteNoWait_removes_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.exchangeDeclareNoWait("test1", BuiltinExchangeType.DIRECT, true,
                        false, false, null);
                channel.exchangeDeleteNoWait("test1", false);
                channel.exchangeDeclarePassive("test1");
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeDelete_does_nothing_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDelete(UUID.randomUUID().toString()));
                Assert.assertNotNull(channel.exchangeDelete(UUID.randomUUID().toString(), false));
                channel.exchangeDeleteNoWait(UUID.randomUUID().toString(), false);
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeBind_binds_two_exchanges() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.queueDeclare());
                Assert.assertNotNull(channel.exchangeBind("ex-to", "ex-from", "test.key"));
                Assert.assertNotNull(channel.queueBind("", "ex-to", "queue.used"));

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", false);
                Assert.assertNotNull(response);
                Assert.assertEquals(new String(response.getBody()), "test message");
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeBindNoWait_binds_two_exchanges() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.queueDeclare());

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                Assert.assertNotNull(channel.queueBind("", "ex-to", "queue.used"));

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                Assert.assertNotNull(response);
                Assert.assertEquals(new String(response.getBody()), "test message");
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeUnbind_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.queueDeclare());

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                Assert.assertNotNull(channel.queueBind("", "ex-to", "queue.used"));

                Assert.assertNotNull(channel.exchangeUnbind("ex-to", "ex-from", "test.key"));

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                Assert.assertNotNull(response);
            }
        }
    }

    @Test(timeOut = timeout)
    public void exchangeUnbindNoWait_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("ex-from", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.exchangeDeclare("ex-to", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.queueDeclare());

                channel.exchangeBindNoWait("ex-to", "ex-from", "test.key", null);
                Assert.assertNotNull(channel.queueBind("", "ex-to", "queue.used"));

                channel.exchangeUnbindNoWait("ex-to", "ex-from", "test.key", null);

                channel.basicPublish("ex-from", "unused", null, "test message".getBytes());
                GetResponse response = channel.basicGet("", true);
                Assert.assertNotNull(response);
            }
        }
    }

    @Test(timeOut = timeout)
    public void queueDeclareNoWait_declares_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.queueDeclareNoWait("", true, false, false, null);
                Assert.assertNotNull(channel.queueDeclarePassive(""));
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = ShutdownSignalException.class)
    public void queueDeclarePassive_throws_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.queueDeclarePassive("test1");
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IOException.class)
    public void queueDelete_deletes_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.queueDeclare());
                Assert.assertNotNull(channel.queueDelete(""));
                channel.queueDeclarePassive("");
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IOException.class)
    public void queueDeleteNoWait_deletes_it() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.queueDeclare());
                channel.queueDeleteNoWait("", false, false);
                channel.queueDeclarePassive("");
            }
        }
    }

    @Test(timeOut = timeout)
    public void queueDelete_does_nothing_when_not_existing() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.queueDelete(UUID.randomUUID().toString()));
                Assert.assertNotNull(channel.queueDelete(UUID.randomUUID().toString(), false, false));
                channel.queueDeleteNoWait(UUID.randomUUID().toString(), false, false);
            }
        }
    }

    @Test(timeOut = timeout)
    public void queuePurge_removes_all_messages() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());
                channel.basicPublish("", queueName, null, "test message".getBytes());

                Assert.assertEquals(channel.messageCount(""), 2);
                Assert.assertNotNull(channel.queuePurge(""));
                Assert.assertEquals(channel.messageCount(""), 0);
            }
        }
    }

    @Test(timeOut = timeout)
    public void basicNack_with_requeue_replaces_message_in_queue() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                GetResponse getResponse = channel.basicGet("", false);

                channel.basicNack(getResponse.getEnvelope().getDeliveryTag(), true, true);

                getResponse = channel.basicGet("", false);

                channel.basicReject(getResponse.getEnvelope().getDeliveryTag(), false);

                Assert.assertNull(channel.basicGet("", false));
            }
        }
    }

    @Test(timeOut = timeout)
    public void basicConsume_with_consumer() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                AtomicBoolean cancelled = new AtomicBoolean();
                String consumerTag = channel.basicConsume("", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.add(new String(body));
                    }

                    @Override
                    public void handleCancelOk(String consumerTag) {
                        cancelled.set(true);
                    }
                });

                TimeUnit.MILLISECONDS.sleep(200L);

                Assert.assertEquals(messages.size(), 1);
                Assert.assertFalse(cancelled.get());
                Assert.assertEquals(channel.consumerCount(""), 1);

                messages.clear();
                channel.basicCancel(consumerTag);

                Assert.assertTrue(cancelled.get());
                Assert.assertEquals(channel.consumerCount(""), 0);

                channel.basicPublish("", queueName, null, "test message".getBytes());
                TimeUnit.MILLISECONDS.sleep(50L);

                Assert.assertEquals(messages.size(), 0);

            }
        }
    }

    @Test(timeOut = timeout)
    public void basicConsume_concurrent_queue_access() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();

                BlockingQueue<String> messages = new LinkedBlockingQueue<>();
                channel.basicConsume("", new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        messages.offer(new String(body));
                    }
                });

                int totalMessages = 101;
                for (int i = 1; i <= totalMessages; i++) {
                    channel.basicPublish("", queueName, null, "test message".getBytes());
                }
                for (int i = 1; i <= totalMessages; i++) {
                    Assert.assertNotNull(messages.poll(200L, TimeUnit.MILLISECONDS));
                }
            }
        }
    }


    @Test(timeOut = timeout)
    public void basicConsume_with_callbacks() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                AtomicBoolean cancelled = new AtomicBoolean();
                channel.basicConsume("",
                        (ct, delivery) -> messages.add(new String(delivery.getBody())),
                        ct -> cancelled.set(true));

                TimeUnit.MILLISECONDS.sleep(200L);

                Assert.assertEquals(messages.size(), 1);
                Assert.assertFalse(cancelled.get());

                channel.queueDelete("");

                Assert.assertTrue(cancelled.get());
            }
        }
    }

    @Test(timeOut = timeout)
    public void basicConsume_with_shutdown_callback() throws IOException, TimeoutException, InterruptedException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message".getBytes());

                List<String> messages = new ArrayList<>();
                channel.basicConsume("",
                        (ct, delivery) -> messages.add(new String(delivery.getBody())),
                        (ct, sig) -> {
                        });

                TimeUnit.MILLISECONDS.sleep(200L);

                Assert.assertEquals(messages.size(), 1);
            }
        }
    }

    @Test(timeOut = timeout)
    public void basicRecover_requeue_all_unacked_messages() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                String queueName = channel.queueDeclare().getQueue();
                channel.basicPublish("", queueName, null, "test message 1".getBytes());
                channel.basicPublish("", queueName, null, "test message 2".getBytes());

                Assert.assertEquals(channel.messageCount(queueName), 2);

                Assert.assertNotNull(channel.basicGet("", false));
                Assert.assertNotNull(channel.basicGet("", false));

                Assert.assertEquals(channel.messageCount(queueName), 0);

                Assert.assertNotNull(channel.basicRecover());

                Assert.assertEquals(channel.messageCount(queueName), 2);
            }
        }
    }

    @Test(timeOut = timeout)
    public void queueUnbind_removes_binding() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                Assert.assertNotNull(channel.exchangeDeclare("ex-test", BuiltinExchangeType.FANOUT));
                Assert.assertNotNull(channel.queueDeclare());
                channel.queueBindNoWait("", "ex-test", "some.key", null);

                channel.basicPublish("ex-test", "unused", null, "test message".getBytes());
                Assert.assertNotNull(channel.basicGet("", false));

                Assert.assertNotNull(channel.queueUnbind("", "ex-test", "some.key"));

                channel.basicPublish("ex-test", "unused", null, "test message".getBytes());
                Assert.assertNull(channel.basicGet("", false));
            }
        }
    }

    @Test(timeOut = timeout)
    public void directReplyTo_basicPublish_basicGet() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();

                channel.basicPublish("", queue, new AMQP.BasicProperties.Builder()
                        .replyTo("amq.rabbitmq.reply-to").build(), "ping".getBytes());

                Assert.assertEquals(channel.messageCount(queue), 1);

                final GetResponse basicGet = channel.basicGet(queue, true);
                final String replyTo = basicGet.getProps().getReplyTo();
                Assert.assertTrue(replyTo.startsWith("amq.gen-"));

                channel.basicPublish("", replyTo, null, "pong".getBytes());

                final GetResponse reply = channel.basicGet("amq.rabbitmq.reply-to", true);
                Assert.assertEquals(new String(reply.getBody()), "pong");
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IOException.class)
    public void directReplyTo_basicPublish_basicConsume() throws IOException, TimeoutException, InterruptedException {
        final String replyTo;
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();

                channel.basicPublish("", queue, new AMQP.BasicProperties.Builder()
                        .replyTo("amq.rabbitmq.reply-to").build(), "ping".getBytes());
                Assert.assertEquals(channel.messageCount(queue), 1);

                final GetResponse basicGet = channel.basicGet(queue, true);
                replyTo = basicGet.getProps().getReplyTo();
                Assert.assertTrue(replyTo.startsWith("amq.gen-"));

                channel.basicPublish("", replyTo, null, "pong".getBytes());

                CountDownLatch latch = new CountDownLatch(1);
                AtomicBoolean cancelled = new AtomicBoolean();
                AtomicReference<String> reply = new AtomicReference<>();
                String consumerTag = channel.basicConsume("amq.rabbitmq.reply-to", true,
                        new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        Assert.assertTrue(reply.compareAndSet(null, new String(body)));
                        latch.countDown();
                    }

                    @Override
                    public void handleCancelOk(String consumerTag) {
                        cancelled.set(true);
                    }
                });

                latch.await(1, TimeUnit.SECONDS);
                channel.basicCancel(consumerTag);

                Assert.assertTrue(cancelled.get());
                Assert.assertEquals(reply.get(), "pong");
            }
        }

        // assert that internal queue is removed
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.queueDeclarePassive(replyTo);
            }
        }
    }

    private static class TrackingCallback implements ReturnCallback {

        int invocationCount;

        @Override
        public void handle(Return returnMessage) {
            invocationCount++;
        }
    }

    @Test(timeOut = timeout)
    public void mandatory_publish_with_multiple_listeners() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                TrackingCallback firstCallbackThatIsRegistedTwice = new TrackingCallback();
                TrackingCallback secondCallbackThatWillBeRemoved = new TrackingCallback();
                TrackingCallback thirdCallback = new TrackingCallback();
                channel.addReturnListener(firstCallbackThatIsRegistedTwice);
                channel.addReturnListener(firstCallbackThatIsRegistedTwice);
                channel.addReturnListener(r -> {
                    throw new RuntimeException("Listener throwing exception");
                });
                ReturnListener returnListener = channel.addReturnListener(secondCallbackThatWillBeRemoved);
                channel.addReturnListener(thirdCallback);
                channel.removeReturnListener(returnListener);
                channel.basicPublish("", "unexisting", true, MessageProperties.BASIC,
                        "msg".getBytes());
                Assert.assertEquals(firstCallbackThatIsRegistedTwice.invocationCount, 1);
                Assert.assertEquals(secondCallbackThatWillBeRemoved.invocationCount, 0);
                Assert.assertEquals(thirdCallback.invocationCount, 1);
            }
        }
    }

    @Test(timeOut = timeout)
    public void rpcClient() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {

                String queue = channel.queueDeclare().getQueue();
                channel.basicConsume(queue, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                               byte[] body) throws IOException {
                        channel.basicPublish(
                                "",
                                properties.getReplyTo(),
                                MessageProperties.BASIC.builder().correlationId(properties.getCorrelationId()).build(),
                                "pong".getBytes()
                        );
                    }
                });

                RpcClientParams params = new RpcClientParams();
                params.channel(channel);
                params.exchange("");
                params.routingKey(queue);
                RpcClient client = new RpcClient(params);
                RpcClient.Response response = client.responseCall("ping".getBytes());
                Assert.assertEquals(response.getBody(), "pong".getBytes());
            }
        }
    }

    @Test(timeOut = timeout)
    public void rpcClientSupportsMandatory() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                RpcClientParams params = new RpcClientParams();
                params.channel(channel);
                params.exchange("");
                params.routingKey("unexistingQueue");
                params.useMandatory(true);
                RpcClient client = new RpcClient(params);
                try {
                    client.responseCall("ping".getBytes());
                    Assert.fail("Expected exception");
                } catch (UnroutableRpcRequestException e) {
                    Assert.assertEquals(e.getReturnMessage().getReplyText(), "No route");
                }
            }
        }
    }


    @Test(timeOut = timeout, expectedExceptions = IllegalStateException.class)
    public void directReplyTo_basicConsume_noAutoAck() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.basicConsume("amq.rabbitmq.reply-to", false, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties properties,
                                               byte[] body) {
                        Assert.fail("not implemented");
                    }

                    @Override
                    public void handleCancelOk(String consumerTag) {
                        Assert.fail("not implemented");
                    }
                });
            }
        }
    }

    @Test(timeOut = timeout, expectedExceptions = IllegalStateException.class)
    public void directReplyTo_basicGet_noAutoAck() throws IOException, TimeoutException {
        try (Connection conn = getConnection()) {
            try (Channel channel = conn.createChannel()) {
                channel.basicGet("amq.rabbitmq.reply-to", false);
            }
        }
    }
}