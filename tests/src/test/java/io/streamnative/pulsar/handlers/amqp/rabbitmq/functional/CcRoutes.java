

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
/**
 * Testcase.
 */
public class CcRoutes extends BrokerTestCase {

    private String[] queues;
    private final String exDirect = "direct_cc_exchange";
    private final String exTopic = "topic_cc_exchange";
    private BasicProperties.Builder propsBuilder;
    protected Map<String, Object> headers;
    private List<String> ccList;
    private List<String> bccList;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        propsBuilder = new BasicProperties.Builder();
        headers = new HashMap<>();
        ccList = new ArrayList<>();
        bccList = new ArrayList<>();
    }

    @Override
    protected void createResources() throws IOException, TimeoutException {
        super.createResources();
        queues = IntStream.range(1, 4)
                .mapToObj(index -> CcRoutes.class.getSimpleName() + "." + UUID.randomUUID().toString())
                .collect(Collectors.toList())
                .toArray(new String[]{});
        for (String q : queues) {
            channel.queueDeclare(q, false, false, true, null);
        }
        channel.exchangeDeclare(exDirect, "direct", false, true, null);
        channel.exchangeDeclare(exTopic, "topic", false, true, null);
    }

    @Override
    protected void releaseResources() throws IOException {
        super.releaseResources();
        for (String q : queues) {
            channel.queueDelete(q);
        }
    }

    //@Test
    public void ccList() throws IOException {
        ccList.add(queue2());
        ccList.add(queue3());
        headerPublish("", queue1(), ccList, null);
        expect(new String[]{queue1(), queue2(), queue3()}, true);
    }

    //@Test
    public void ccIgnoreEmptyAndInvalidRoutes() throws IOException {
        bccList.add("frob");
        headerPublish("", queue1(), ccList, bccList);
        expect(new String[]{queue1()}, true);
    }

    //@Test
    public void bcc() throws IOException {
        bccList.add(queue2());
        headerPublish("", queue1(), null, bccList);
        expect(new String[]{queue1(), queue2()}, false);
    }

    //@Test
    public void noDuplicates() throws IOException {
        ccList.add(queue1());
        ccList.add(queue1());
        bccList.add(queue1());
        headerPublish("", queue1(), ccList, bccList);
        expect(new String[]{queue1()}, true);
    }

    //@Test
    public void directExchangeWithoutBindings() throws IOException {
        ccList.add(queue1());
        headerPublish(exDirect, queue2(), ccList, null);
        expect(new String[]{}, true);
    }

    //@Test
    public void topicExchange() throws IOException {
        ccList.add("routing_key");
        channel.queueBind(queue2(), exTopic, "routing_key");
        headerPublish(exTopic, "", ccList, null);
        expect(new String[]{queue2()}, true);
    }

    //@Test
    public void boundExchanges() throws IOException {
        ccList.add("routing_key1");
        bccList.add("routing_key2");
        channel.exchangeBind(exTopic, exDirect, "routing_key1");
        channel.queueBind(queue2(), exTopic, "routing_key2");
        headerPublish(exDirect, "", ccList, bccList);
        expect(new String[]{queue2()}, true);
    }

    //@Test
    public void nonArray() throws IOException {
        headers.put("CC", 0);
        propsBuilder.headers(headers);
        channel.basicPublish("", queue1(), propsBuilder.build(), new byte[0]);
        try {
            expect(new String[]{}, false);
            fail();
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    private void headerPublish(String ex, String to, List<String> cc, List<String> bcc) throws IOException {
        if (cc != null) {
            headers.put("CC", ccList);
        }
        if (bcc != null) {
            headers.put("BCC", bccList);
        }
        propsBuilder.headers(headers);
        channel.basicPublish(ex, to, propsBuilder.build(), new byte[0]);
    }

    private void expect(String[] expectedQueues, boolean usedCc) throws IOException {
        GetResponse getResponse;
        List<String> expectedList = Arrays.asList(expectedQueues);
        for (String q : queues) {
            getResponse = basicGet(q);
            if (expectedList.contains(q)) {
                assertNotNull(getResponse);
                assertEquals(0, getResponse.getMessageCount());
                Map<?, ?> headers = getResponse.getProps().getHeaders();
                if (headers != null) {
                    assertEquals(usedCc, headers.containsKey("CC"));
                    assertFalse(headers.containsKey("BCC"));
                }
            } else {
                assertNull(getResponse);
            }
        }
    }

    String queue1() {
        return queues[0];
    }

    String queue2() {
        return queues[1];
    }

    String queue3() {
        return queues[2];
    }
}
