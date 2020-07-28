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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.routing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.model.AlternateBinding;
import org.junit.Ignore;
import org.junit.Test;

/**
 * AlternateBindingRoutingTest.
 */
public class AlternateBindingRoutingTest extends JmsTestBase
{
    @Test
    @Ignore
    public void testFanoutExchangeAsAlternateBinding() throws Exception
    {
        String queueName = getTestName();
        String deadLetterQueueName = queueName + "_DeadLetter";
        String deadLetterExchangeName = "deadLetterExchange";

        createEntityUsingAmqpManagement(deadLetterQueueName,
                                        "org.apache.qpid.StandardQueue",
                                        Collections.emptyMap());
        createEntityUsingAmqpManagement(deadLetterExchangeName,
                                        "org.apache.qpid.FanoutExchange",
                                        Collections.emptyMap());

        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", deadLetterQueueName);
        arguments.put("bindingKey", queueName);
        performOperationUsingAmqpManagement(deadLetterExchangeName,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            arguments);

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.NAME, queueName);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(Collections.singletonMap(AlternateBinding.DESTINATION,
                                                                                      deadLetterExchangeName)));
        createEntityUsingAmqpManagement(queueName,
                                        "org.apache.qpid.StandardQueue",
                                        attributes);

        Queue testQueue = createQueue(queueName);

        Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            Utils.sendMessages(session, testQueue, 1);

            assertEquals("Unexpected number of messages on queue", 1, getQueueDepth(queueName));
            assertEquals("Unexpected number of messages on DLQ", 0, getQueueDepth(deadLetterQueueName));

            performOperationUsingAmqpManagement(queueName,
                                                "DELETE",
                                                "org.apache.qpid.Queue",
                                                Collections.emptyMap());

            assertEquals("Unexpected number of messages on DLQ after deletion", 1, getQueueDepth(deadLetterQueueName));
        }
        finally
        {
            connection.close();
        }
    }

    private int getQueueDepth(final String queueName) throws Exception
    {
        Map<String, Object> arguments =
                Collections.singletonMap("statistics", Collections.singletonList("queueDepthMessages"));
        Object statistics = performOperationUsingAmqpManagement(queueName,
                                                                "getStatistics",
                                                                "org.apache.qpid.Queue",
                                                                arguments);
        assertNotNull("Statistics is null", statistics);
        assertTrue("Statistics is not map", statistics instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        assertTrue("queueDepthMessages is not present", statisticsMap.get("queueDepthMessages") instanceof Number);
        return ((Number) statisticsMap.get("queueDepthMessages")).intValue();
    }
}
