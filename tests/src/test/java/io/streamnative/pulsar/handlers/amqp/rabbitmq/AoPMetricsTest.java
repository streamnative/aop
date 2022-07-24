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

import static org.awaitility.Awaitility.await;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import io.streamnative.pulsar.handlers.amqp.AmqpTestBase;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * AoP metrics test.
 */
@Slf4j
public class AoPMetricsTest extends AmqpTestBase {

    private final OkHttpClient client = new OkHttpClient().newBuilder().build();

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.setup();
    }

    @Test(timeOut = 1000 * 5)
    public void metricsTest() throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setVirtualHost("vhost1");
        connectionFactory.setPort(getAmqpBrokerPortList().get(0));
        Connection connection1 = connectionFactory.newConnection();

        ConnectionFactory connectionFactory2 = new ConnectionFactory();
        connectionFactory2.setVirtualHost("vhost2");
        connectionFactory2.setPort(getAopProxyPortList().get(0));
        Connection connection2 = connectionFactory2.newConnection();
        Connection connection3 = connectionFactory2.newConnection();

        String metrics = getMetrics();
        Map<String, String> expectedMetrics = new HashMap<>();
        expectedMetrics.put("test", "2.0");
        Assert.assertEquals(getLabelValue(metrics, "vhost_count", "cluster"), expectedMetrics);

        expectedMetrics.clear();
        expectedMetrics.put("vhost1", "1.0");
        expectedMetrics.put("vhost2", "2.0");
        Assert.assertEquals(getLabelValue(metrics, "amqp_connection_counter", "vhost"), expectedMetrics);

        connection1.close();
        connection2.close();

        expectedMetrics.clear();
        expectedMetrics.put("vhost1", "0.0");
        expectedMetrics.put("vhost2", "1.0");

        metrics = getMetrics();
        Assert.assertEquals(getLabelValue(metrics, "amqp_connection_counter", "vhost"), expectedMetrics);

        Channel channel = connection3.createChannel();

        String ex1 = "ex1";
        String ex2 = "ex2";
        channel.exchangeDeclare(ex1, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(ex2, BuiltinExchangeType.DIRECT);

        metrics = getMetrics();
        expectedMetrics.clear();
        expectedMetrics.put("ex1", "0.0");
        expectedMetrics.put("ex2", "0.0");
        Assert.assertEquals(getLabelValue(metrics, "exchange_write_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_write_failed_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_read_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_read_failed_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_ack_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_route_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_route_failed_counter", "exchange_name"), expectedMetrics);

        String qu1 = "qu1";
        String qu2 = "qu2";
        String qu3 = "qu3";
        channel.queueDeclare(qu1, true, false, false, null);
        channel.queueDeclare(qu2, true, false, false, null);
        channel.queueDeclare(qu3, true, false, false, null);
        metrics = getMetrics();
        expectedMetrics.clear();
        expectedMetrics.put("qu1", "0.0");
        expectedMetrics.put("qu2", "0.0");
        expectedMetrics.put("qu3", "0.0");
        Assert.assertEquals(getLabelValue(metrics, "queue_write_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_write_failed_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_dispatch_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_read_failed_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_ack_counter", "queue_name"), expectedMetrics);

        channel.queueBind(qu1, ex1, qu1);
        int messageCount = 100;
        Set<String> messageSet = new HashSet<>();
        for (int i = 0; i < messageCount; i++) {
            String msg = "msg-" + i;
            messageSet.add(msg);
            channel.basicPublish(ex1, qu1, null, msg.getBytes());
        }

        channel.basicConsume(qu1, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("receive msg " + new String(body));
                messageSet.remove(new String(body));
            }
        });
        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(100, TimeUnit.MILLISECONDS).until(messageSet::isEmpty);
        metrics = getMetrics();
        System.out.println(metrics);
        expectedMetrics.clear();
        expectedMetrics.put("ex1", "100.0");
        expectedMetrics.put("ex2", "0.0");

        Assert.assertEquals(getLabelValue(metrics, "exchange_write_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_route_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_read_counter", "exchange_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "exchange_ack_counter", "exchange_name"), expectedMetrics);

        expectedMetrics.clear();
        expectedMetrics.put("qu1", "100.0");
        expectedMetrics.put("qu2", "0.0");
        expectedMetrics.put("qu3", "0.0");
        Assert.assertEquals(getLabelValue(metrics, "queue_write_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_dispatch_counter", "queue_name"), expectedMetrics);
        Assert.assertEquals(getLabelValue(metrics, "queue_ack_counter", "queue_name"), expectedMetrics);

        connection3.close();
    }

    private Map<String, String> getLabelValue(String metrics, String metricsName, String key) throws Exception {
        Map<String, String> map = new HashMap<>();
        for (String m : metrics.split("\n")) {
            if (m.startsWith(metricsName)) {
                String[] metricsArr = m.split(" ");
                String labels = metricsArr[0];
                String value = metricsArr[1];
                Gson gson = new Gson();
                Map<String, String> labelMap = gson.fromJson(labels.replace(metricsName, ""), Map.class);
                map.put(labelMap.get(key), value);
            }
        }
        return map;
    }

    private String getMetrics() throws Exception {
        Request request = new Request.Builder().get()
                .url("http://localhost:" + getBrokerWebservicePortList().get(0) + "/metrics").build();
        try (Response response = client.newCall(request).execute()) {
            return response.body().string();
        }
    }

}
