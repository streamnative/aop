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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.utility.BlockingCell;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Routing.
 */
public class Routing extends BrokerTestCase {

    protected final String exchange = "MRDQ";
    protected final String queue1 = "foo";
    protected final String queue2 = "bar";

    private volatile BlockingCell<Integer> returnCell;

    protected void createResources() throws IOException {
        channel.exchangeDeclare(exchange, "direct");
        channel.queueDeclare(queue1, false, false, false, null);
        channel.queueDeclare(queue2, false, false, false, null);
    }

    protected void releaseResources() throws IOException {
        channel.queueDelete(queue1);
        channel.queueDelete(queue2);
        channel.exchangeDelete(exchange);
    }

    private void bind(String queue, String routingKey)
            throws IOException {
        channel.queueBind(queue, exchange, routingKey);
    }

    private void check(String routingKey, boolean expectqueue1, boolean expectqueue2)
            throws IOException {
        channel.basicPublish(exchange, routingKey, null, "mrdq".getBytes());
        checkGet(queue1, expectqueue1);
        checkGet(queue2, expectqueue2);
    }

    private void checkGet(String queue, boolean messageExpected)
            throws IOException {
        GetResponse r = channel.basicGet(queue, true);
        if (messageExpected) {
            assertNotNull(r);
        } else {
            assertNull(r);
        }
    }

    /**
     * Tests the "default queue name" and "default routing key" pieces
     * of the spec. See the doc for the "queue" and "routing key"
     * fields of queue.bind.
     */
    ////@Test
    public void mRDQRouting()
            throws IOException {
        bind(queue1, "baz");        //queue1, "baz"
        bind(queue1, "");           //queue1, ""
        bind("", "baz");        //queue2, "baz"
        bind("", "");           //queue2, queue2
        check("", true, false);
        check(queue1, false, false);
        check(queue2, false, true);
        check("baz", true, true);
    }

    /**
     * If a queue has more than one binding to an exchange, it should
     * NOT receive duplicate copies of a message that matches both
     * bindings.
     */
    ////@Test
    public void doubleBinding()
            throws IOException {
        channel.queueBind(queue1, "amq.topic", "x.#");
        channel.queueBind(queue1, "amq.topic", "#.x");
        channel.basicPublish("amq.topic", "x.y", null, "x.y".getBytes());
        checkGet(queue1, true);
        checkGet(queue1, false);
        channel.basicPublish("amq.topic", "y.x", null, "y.x".getBytes());
        checkGet(queue1, true);
        checkGet(queue1, false);
        channel.basicPublish("amq.topic", "x.x", null, "x.x".getBytes());
        checkGet(queue1, true);
        checkGet(queue1, false);
    }

    ////@Test
    public void fanoutRouting() throws Exception {

        List<String> queues = new ArrayList<String>();

        for (int i = 0; i < 2; i++) {
            String q = "Q-" + System.nanoTime();
            channel.queueDeclare(q, false, true, true, null);
            channel.queueBind(q, "amq.fanout", "");
            queues.add(q);
        }

        channel.basicPublish("amq.fanout", System.nanoTime() + "",
                null, "fanout".getBytes());

        for (String q : queues) {
            checkGet(q, true);
        }

        for (String q : queues) {
            channel.queueDelete(q);
        }
    }

    ////@Test
    public void topicRouting() throws Exception {

        List<String> queues = new ArrayList<String>();

        //100+ queues is the trigger point for bug20046
        for (int i = 0; i < 100; i++) {
            channel.queueDeclare();
            AMQP.Queue.DeclareOk ok = channel.queueDeclare();
            String q = ok.getQueue();
            channel.queueBind(q, "amq.topic", "#");
            queues.add(q);
        }

        channel.basicPublish("amq.topic", "", null, "topic".getBytes());

        for (String q : queues) {
            checkGet(q, true);
        }
    }

    ////@Test
    public void headersRouting() throws Exception {
        Map<String, Object> spec = new HashMap<String, Object>();
        spec.put("h1", "12345");
        spec.put("h2", "bar");
        spec.put("h3", null);
        spec.put("x-match", "all");
        channel.queueBind(queue1, "amq.match", "", spec);
        spec.put("x-match", "any");
        channel.queueBind(queue2, "amq.match", "", spec);

        AMQP.BasicProperties.Builder props = new AMQP.BasicProperties.Builder();

        channel.basicPublish("amq.match", "", null, "0".getBytes());
        channel.basicPublish("amq.match", "", props.build(), "0b".getBytes());

        Map<String, Object> map = new HashMap<String, Object>();
        props.headers(map);

        map.clear();
        map.put("h1", "12345");
        channel.basicPublish("amq.match", "", props.build(), "1".getBytes());

        map.clear();
        map.put("h1", "12345");
        channel.basicPublish("amq.match", "", props.build(), "1b".getBytes());

        map.clear();
        map.put("h2", "bar");
        channel.basicPublish("amq.match", "", props.build(), "2".getBytes());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "bar");
        channel.basicPublish("amq.match", "", props.build(), "3".getBytes());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "bar");
        map.put("h3", null);
        channel.basicPublish("amq.match", "", props.build(), "4".getBytes());

        map.clear();
        map.put("h1", "12345");
        map.put("h2", "quux");
        channel.basicPublish("amq.match", "", props.build(), "5".getBytes());

        map.clear();
        map.put("h1", "zot");
        map.put("h2", "quux");
        map.put("h3", null);
        channel.basicPublish("amq.match", "", props.build(), "6".getBytes());

        map.clear();
        map.put("h3", null);
        channel.basicPublish("amq.match", "", props.build(), "7".getBytes());

        map.clear();
        map.put("h1", "zot");
        map.put("h2", "quux");
        channel.basicPublish("amq.match", "", props.build(), "8".getBytes());

        checkGet(queue1, true); // 4
        checkGet(queue1, false);

        checkGet(queue2, true); // 1
        checkGet(queue2, true); // 2
        checkGet(queue2, true); // 3
        checkGet(queue2, true); // 4
        checkGet(queue2, true); // 5
        checkGet(queue2, true); // 6
        checkGet(queue2, true); // 7
        checkGet(queue2, true); // 8
        checkGet(queue2, false);
    }

    ////@Test
    public void basicReturn() throws IOException {
        channel.addReturnListener(makeReturnListener());
        returnCell = new BlockingCell<Integer>();

        //returned 'mandatory' publish
        channel.basicPublish("", "unknown", true, false, null, "mandatory1".getBytes());
        checkReturn(AMQP.NO_ROUTE);

        //routed 'mandatory' publish
        channel.basicPublish("", queue1, true, false, null, "mandatory2".getBytes());
        assertNotNull(channel.basicGet(queue1, true));

        //'immediate' publish
        channel.basicPublish("", queue1, false, true, null, "immediate".getBytes());
        try {
            channel.basicQos(0); //flush
            fail("basic.publish{immediate=true} should not be supported");
        } catch (IOException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        } catch (AlreadyClosedException ioe) {
            checkShutdownSignal(AMQP.NOT_IMPLEMENTED, ioe);
        }
    }

    ////@Test
    public void basicReturnTransactional() throws IOException {
        channel.txSelect();
        channel.addReturnListener(makeReturnListener());
        returnCell = new BlockingCell<Integer>();

        //returned 'mandatory' publish
        channel.basicPublish("", "unknown", true, false, null, "mandatory1".getBytes());
        try {
            returnCell.uninterruptibleGet(200);
            fail("basic.return issued prior to tx.commit");
        } catch (TimeoutException toe) {
        }
        channel.txCommit();
        checkReturn(AMQP.NO_ROUTE);

        //routed 'mandatory' publish
        channel.basicPublish("", queue1, true, false, null, "mandatory2".getBytes());
        channel.txCommit();
        assertNotNull(channel.basicGet(queue1, true));

        //returned 'mandatory' publish when message is routable on
        //publish but not on commit
        channel.basicPublish("", queue1, true, false, null, "mandatory2".getBytes());
        channel.queueDelete(queue1);
        channel.txCommit();
        checkReturn(AMQP.NO_ROUTE);
        channel.queueDeclare(queue1, false, false, false, null);
    }

    protected ReturnListener makeReturnListener() {
        return new ReturnListener() {
            public void handleReturn(int replyCode,
                                     String replyText,
                                     String exchange,
                                     String routingKey,
                                     AMQP.BasicProperties properties,
                                     byte[] body)
                    throws IOException {
                Routing.this.returnCell.set(replyCode);
            }
        };
    }

    protected void checkReturn(int replyCode) {
        assertEquals((int) returnCell.uninterruptibleGet(), AMQP.NO_ROUTE);
        returnCell = new BlockingCell<Integer>();
    }

}
