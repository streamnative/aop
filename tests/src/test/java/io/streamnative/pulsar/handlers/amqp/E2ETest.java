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
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Admin API test.
 */
public class E2ETest extends AmqpTestBase{

    @Test()
    public void listExchangeTest() throws Exception {
        Connection connection = getConnection("vhost1", true);
        Channel channel = connection.createChannel();
        String ex1, ex2, qu1, qu2, qu3;
        ex1 = "ex1";
        ex2 = "ex2";
        qu1 = "ex1_1";
        qu2 = "ex2_1";
        qu3 = "ex3_1";
        channel.exchangeDeclare(ex1, BuiltinExchangeType.FANOUT, true);
        channel.exchangeDeclare(ex2, BuiltinExchangeType.TOPIC, true);

//        Thread.sleep(1000 * 60 * 60);

        channel.exchangeBind(ex2, ex1, "");

        channel.queueDeclare(qu1, true, false, false, null);
        channel.queueDeclare(qu2, true, false, false, null);
        channel.queueDeclare(qu3, true, false, false, null);
        channel.queueBind(qu1, ex1, "");
        channel.queueBind(qu2, ex2, "color.*");
        channel.queueBind(qu3, ex2, "sharp.*");

        channel.basicConsume(qu1, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[qu1] receive msg: " + new String(body));
            }
        });
        channel.basicConsume(qu2, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[qu2] receive msg: " + new String(body));
            }
        });
        channel.basicConsume(qu3, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[qu3] receive msg: " + new String(body));
            }
        });

        for (int i = 0; i < 10; i++) {
            String key = "color.red";
            channel.basicPublish(ex1, key, null, ("[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "color.yellow";
            channel.basicPublish(ex1, key, null, ("[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "sharp.square";
            channel.basicPublish(ex1, key, null, ("[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "sharp.yellow";
            channel.basicPublish(ex1, key, null, ("[" + i + "]" + key).getBytes());
        }

        Thread.sleep(1000 * 5);
        channel.exchangeUnbind(ex2, ex1, "");

        for (int i = 0; i < 10; i++) {
            String key = "color.red";
            channel.basicPublish(ex1, key, null, ("++[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "color.yellow";
            channel.basicPublish(ex1, key, null, ("++[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "sharp.square";
            channel.basicPublish(ex1, key, null, ("++[" + i + "]" + key).getBytes());
        }
        for (int i = 0; i < 10; i++) {
            String key = "sharp.yellow";
            channel.basicPublish(ex1, key, null, ("++[" + i + "]" + key).getBytes());
        }

        System.out.println("finish publish messages");
    }

    @Test()
    public void pinTest() throws Exception {
        @Cleanup
        Connection connection = getConnection("vhost1", true);
        @Cleanup
        Channel channel = connection.createChannel();
        String predicationInput, predicationInputHeaders, verificationInput, analyticsInput, finalizingInput;
        predicationInput = "prediction-input";
        predicationInputHeaders = "prediction-input.headers";
        verificationInput = "verification-input";
        analyticsInput = "analytics-input";
        finalizingInput = "finalizing-input";
        channel.exchangeDeclare(predicationInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(predicationInputHeaders, BuiltinExchangeType.HEADERS, true);
        channel.exchangeDeclare(verificationInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(analyticsInput, BuiltinExchangeType.TOPIC, true);
        channel.exchangeDeclare(finalizingInput, BuiltinExchangeType.TOPIC, true);

        channel.exchangeBind(predicationInputHeaders, predicationInput, "#");

        Map<String, Object> map1 = new HashMap<>();
        map1.put("verification-requested", true);
        channel.exchangeBind(verificationInput, predicationInputHeaders, "", map1);

        Map<String, Object> map2 = new HashMap<>();
        map2.put("analytics-requested", true);
        channel.exchangeBind(analyticsInput, predicationInputHeaders, "", map2);

        Map<String, Object> map3 = new HashMap<>();
        map3.put("finalizing", true);
        channel.exchangeBind(finalizingInput, predicationInputHeaders, "", map3);

        Set<String> messageSet = new HashSet<>();

        String authPolicyWorker = "AuthPolicyWorker";
        channel.queueDeclare(authPolicyWorker, true, false, false, null);
        channel.queueBind(authPolicyWorker, verificationInput, "*.*.prediction.deviceprint");
        channel.queueBind(authPolicyWorker, analyticsInput, "*.*.prediction.fraud_risk_grouper");
        channel.basicConsume(authPolicyWorker, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[" + authPolicyWorker + "] receive msg: " + new String(body));
                messageSet.remove(new String(body));
            }
        });

        String finalizingQueue = "FinalizingQueue";
        channel.queueDeclare(finalizingQueue, true, false, false, null);
        channel.queueBind(finalizingQueue, finalizingInput, "");
        channel.basicConsume(finalizingQueue, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[" + finalizingQueue + "] receive msg: " + new String(body));
                messageSet.remove(new String(body));
            }
        });

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            Map<String, Object> headers = new HashMap<>();
            headers.put("verification-requested", true);
            basicProperties.headers(headers);
            String key = "a.b.prediction.deviceprint";
            String msg = "[" + i + "] with key " + key;
            messageSet.add(msg);
            channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            Map<String, Object> headers = new HashMap<>();
            headers.put("analytics-requested", true);
            basicProperties.headers(headers);
            String key = "x.y.prediction.fraud_risk_grouper";
            String msg = "[" + i + "] with key " + key;
            messageSet.add(msg);
            channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
        }

        for (int i = 0; i < 10; i++) {
            AMQP.BasicProperties.Builder basicProperties = new AMQP.BasicProperties().builder();
            Map<String, Object> headers = new HashMap<>();
            headers.put("finalizing", true);
            basicProperties.headers(headers);
            String key = "FinalizingQueue";
            String msg = "[" + i + "] with key " + key;
            messageSet.add(msg);
            channel.basicPublish(predicationInput, key, basicProperties.build(), msg.getBytes());
        }

        System.out.println("finish publish messages");
        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.SECONDS).until(messageSet::isEmpty);
    }

}
