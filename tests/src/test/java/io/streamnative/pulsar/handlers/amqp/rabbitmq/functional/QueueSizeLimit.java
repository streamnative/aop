


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
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Test queue max length limit.
 */
public class QueueSizeLimit extends BrokerTestCase {

    private final int MAXMAXLENGTH = 3;
    private final String q = "queue-maxlength";

    @Test
    public void queueSize() throws IOException, InterruptedException {
        for (int maxLen = 0; maxLen <= MAXMAXLENGTH; maxLen++) {
            setupNonDlxTest(maxLen, false);
            assertHead(maxLen, "msg2", q);
            deleteQueue(q);
        }
    }

    @Test
    public void queueSizeUnacked() throws IOException, InterruptedException {
        for (int maxLen = 0; maxLen <= MAXMAXLENGTH; maxLen++) {
            setupNonDlxTest(maxLen, true);
            assertHead(maxLen > 0 ? 1 : 0, "msg" + (maxLen + 1), q);
            deleteQueue(q);
        }
    }

    @Test
    public void queueSizeDlx() throws IOException, InterruptedException {
        for (int maxLen = 0; maxLen <= MAXMAXLENGTH; maxLen++) {
            setupDlxTest(maxLen, false);
            assertHead(1, "msg1", "DLQ");
            deleteQueue(q);
            deleteQueue("DLQ");
        }
    }

    @Test
    public void queueSizeUnackedDlx() throws IOException, InterruptedException {
        for (int maxLen = 0; maxLen <= MAXMAXLENGTH; maxLen++) {
            setupDlxTest(maxLen, true);
            assertHead(maxLen > 0 ? 0 : 1, "msg1", "DLQ");
            deleteQueue(q);
            deleteQueue("DLQ");
        }
    }

    @Test
    public void requeue() throws IOException, InterruptedException {
        for (int maxLen = 1; maxLen <= MAXMAXLENGTH; maxLen++) {
            declareQueue(maxLen, false);
            setupRequeueTest(maxLen);
            assertHead(maxLen, "msg1", q);
            deleteQueue(q);
        }
    }

    @Test
    public void requeueWithDlx() throws IOException, InterruptedException {
        for (int maxLen = 1; maxLen <= MAXMAXLENGTH; maxLen++) {
            declareQueue(maxLen, true);
            setupRequeueTest(maxLen);
            assertHead(maxLen, "msg1", q);
            assertHead(maxLen, "msg1", "DLQ");
            deleteQueue(q);
            deleteQueue("DLQ");
        }
    }

    private void setupNonDlxTest(int maxLen, boolean unAcked) throws IOException, InterruptedException {
        declareQueue(maxLen, false);
        fill(maxLen);
        if (unAcked) {
            getUnacked(maxLen);
        }
        publish("msg" + (maxLen + 1));
    }

    private void setupDlxTest(int maxLen, boolean unAcked) throws IOException, InterruptedException {
        declareQueue(maxLen, true);
        fill(maxLen);
        if (unAcked) {
            getUnacked(maxLen);
        }
        publish("msg" + (maxLen + 1));
        try {
            Thread.sleep(100);
        } catch (InterruptedException _e) {
        }
    }

    private void setupRequeueTest(int maxLen) throws IOException, InterruptedException {
        fill(maxLen);
        List<Long> tags = getUnacked(maxLen);
        fill(maxLen);
        channel.basicNack(tags.get(0), false, true);
        if (maxLen > 1) {
            channel.basicNack(tags.get(maxLen - 1), true, true);
        }
    }

    private void declareQueue(int maxLen, boolean dlx) throws IOException {
        Map<String, Object> args = new HashMap<String, Object>();
        args.put("x-max-length", maxLen);
        if (dlx) {
            args.put("x-dead-letter-exchange", "amq.fanout");
            channel.queueDeclare("DLQ", false, true, false, null);
            channel.queueBind("DLQ", "amq.fanout", "");
        }
        channel.queueDeclare(q, false, true, true, args);
    }

    private void fill(int count) throws IOException, InterruptedException {
        for (int i = 1; i <= count; i++) {
            publish("msg" + i);
        }
    }

    private void publish(String payload) throws IOException, InterruptedException {
        basicPublishVolatile(payload.getBytes(), q);
    }

    private void assertHead(int expectedLength, String expectedHeadPayload, String queueName) throws IOException {
        GetResponse head = channel.basicGet(queueName, true);
        if (expectedLength > 0) {
            assertNotNull(head);
            assertEquals(expectedHeadPayload, new String(head.getBody()));
            assertEquals(expectedLength, head.getMessageCount() + 1);
        } else {
            assertNull(head);
        }
    }

    private List<Long> getUnacked(int howMany) throws IOException {
        List<Long> tags = new ArrayList<Long>(howMany);
        for (; howMany > 0; howMany--) {
            GetResponse response = channel.basicGet(q, false);
            tags.add(response.getEnvelope().getDeliveryTag());
        }
        return tags;
    }
}
