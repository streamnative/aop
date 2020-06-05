
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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static io.streamnative.pulsar.handlers.amqp.rabbitmq.functional.TestQos.drain;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.QueueingConsumer;
import com.rabbitmq.client.test.QueueingConsumer.Delivery;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

/**
 * PerConsumerPrefetch.
 */
public class PerConsumerPrefetch extends BrokerTestCase {
    private String q = generateQueueName();
    private String x = generateExchangeName();
    private String r = "key-1";

    @Override
    protected void createResources() throws IOException {
        declareExchangeAndQueueToBind(q, x, r);
    }

    private interface Closure {
        void makeMore(List<Delivery> deliveries) throws IOException;
    }

    @Test
    public void singleAck() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                for (Delivery del : deliveries) {
                    ack(del, false);
                }
            }
        });
    }

    @Test
    public void multiAck() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                ack(deliveries.get(deliveries.size() - 1), true);
            }
        });
    }

    @Test
    public void singleNack() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                for (Delivery del : deliveries) {
                    nack(del, false, false);
                }
            }
        });

    }

    @Test
    public void multiNack() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                nack(deliveries.get(deliveries.size() - 1), true, true);
            }
        });

    }

    @Test
    public void recover() throws IOException {
        testPrefetch(new Closure() {
            public void makeMore(List<Delivery> deliveries) throws IOException {
                channel.basicRecover();
            }
        });
    }

    private void testPrefetch(Closure closure) throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 15);
        consume(c, 5, false);
        List<Delivery> deliveries = drain(c, 5);

        //ack(channel.basicGet(q, false), false);
        drain(c, 0);

        closure.makeMore(deliveries);
        drain(c, 5);
    }

    //@Test
    public void prefetchOnEmpty() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 5);
        consume(c, 10, false);
        drain(c, 5);
        publish(q, 10);
        drain(c, 5);
    }

    //@Test
    public void autoAckIgnoresPrefetch() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 10);
        consume(c, 1, true);
        drain(c, 10);
    }

    //@Test
    public void prefetchZeroMeansInfinity() throws IOException {
        QueueingConsumer c = new QueueingConsumer(channel);
        publish(q, 10);
        consume(c, 0, false);
        drain(c, 10);
    }

    private void publish(String q, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            channel.basicPublish(x, r, null, "1".getBytes());
        }
    }

    private void consume(QueueingConsumer c, int prefetch, boolean autoAck) throws IOException {
        channel.basicQos(prefetch);
        channel.basicConsume(q, autoAck, c);
    }

    private void ack(Delivery del, boolean multi) throws IOException {
        channel.basicAck(del.getEnvelope().getDeliveryTag(), multi);
    }

    private void ack(GetResponse get, boolean multi) throws IOException {
        channel.basicAck(get.getEnvelope().getDeliveryTag(), multi);
    }

    private void nack(Delivery del, boolean multi, boolean requeue) throws IOException {
        channel.basicNack(del.getEnvelope().getDeliveryTag(), multi, requeue);
    }
}
