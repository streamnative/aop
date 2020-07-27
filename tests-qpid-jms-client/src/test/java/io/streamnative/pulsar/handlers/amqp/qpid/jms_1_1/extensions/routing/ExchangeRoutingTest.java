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
import javax.jms.Destination;
import javax.jms.Session;
import org.apache.qpid.server.model.Binding;
import org.apache.qpid.server.model.Protocol;
import org.junit.Ignore;
import org.junit.Test;

/**
 * ExchangeRoutingTest.
 */
@Ignore
public class ExchangeRoutingTest extends JmsTestBase
{

    private static final String AMQP_MNG_QPID_EXCHANGE_DIRECT = "org.apache.qpid.DirectExchange";
    private static final String AMQP_MNG_QPID_QUEUE_STANDARD = "org.apache.qpid.StandardQueue";

    @Test
    public void testExchangeToQueueRouting() throws Exception
    {
        String queueName = getTestName() + "Queue";
        String exchangeName = getTestName() + "Exchange";
        String routingKey = "key";

        createEntityUsingAmqpManagement(queueName, AMQP_MNG_QPID_QUEUE_STANDARD, Collections.emptyMap());
        createEntityUsingAmqpManagement(exchangeName, AMQP_MNG_QPID_EXCHANGE_DIRECT, Collections.emptyMap());

        final Map<String, Object> bindingArguments = new HashMap<>();
        bindingArguments.put("destination", queueName);
        bindingArguments.put("bindingKey", routingKey);

        performOperationUsingAmqpManagement(exchangeName,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            bindingArguments);

        routeTest(exchangeName, queueName, "unboundKey", 0, 0);
        routeTest(exchangeName, queueName, routingKey, 0, 1);
    }

    @Test
    public void testExchangeToExchangeToQueueRouting() throws Exception
    {
        String queueName = getTestName() + "Queue";
        String exchangeName1 = getTestName() + "Exchange1";
        String exchangeName2 = getTestName() + "Exchange2";
        String bindingKey = "key";

        createEntityUsingAmqpManagement(queueName, AMQP_MNG_QPID_QUEUE_STANDARD, Collections.emptyMap());
        createEntityUsingAmqpManagement(exchangeName1, AMQP_MNG_QPID_EXCHANGE_DIRECT, Collections.emptyMap());
        createEntityUsingAmqpManagement(exchangeName2, AMQP_MNG_QPID_EXCHANGE_DIRECT, Collections.emptyMap());

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", exchangeName2);
        binding1Arguments.put("bindingKey", bindingKey);

        performOperationUsingAmqpManagement(exchangeName1,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            binding1Arguments);

        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", queueName);
        binding2Arguments.put("bindingKey", bindingKey);

        performOperationUsingAmqpManagement(exchangeName2,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            binding2Arguments);

        routeTest(exchangeName1, queueName, bindingKey, 0, 1);
    }

    @Test
    public void testExchangeToExchangeToQueueRoutingWithReplacementRoutingKey() throws Exception
    {
        String queueName = getTestName() + "Queue";
        String exchangeName1 = getTestName() + "Exchange1";
        String exchangeName2 = getTestName() + "Exchange2";
        String bindingKey1 = "key1";
        String bindingKey2 = "key2";

        createEntityUsingAmqpManagement(queueName, AMQP_MNG_QPID_QUEUE_STANDARD, Collections.emptyMap());
        createEntityUsingAmqpManagement(exchangeName1, AMQP_MNG_QPID_EXCHANGE_DIRECT, Collections.emptyMap());
        createEntityUsingAmqpManagement(exchangeName2, AMQP_MNG_QPID_EXCHANGE_DIRECT, Collections.emptyMap());

        final Map<String, Object> binding1Arguments = new HashMap<>();
        binding1Arguments.put("destination", exchangeName2);
        binding1Arguments.put("bindingKey", bindingKey1);
        binding1Arguments.put("arguments",
                              new ObjectMapper().writeValueAsString(Collections.singletonMap(Binding.BINDING_ARGUMENT_REPLACEMENT_ROUTING_KEY,
                                                                                             bindingKey2)));

        performOperationUsingAmqpManagement(exchangeName1,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            binding1Arguments);

        final Map<String, Object> binding2Arguments = new HashMap<>();
        binding2Arguments.put("destination", queueName);
        binding2Arguments.put("bindingKey", bindingKey2);

        performOperationUsingAmqpManagement(exchangeName2,
                                            "bind",
                                            "org.apache.qpid.Exchange",
                                            binding2Arguments);

        routeTest(exchangeName1, queueName, bindingKey1, 0, 1);
    }

    private void routeTest(final String fromExchangeName,
                           final String queueName,
                           final String routingKey,
                           final int expectedDepthBefore,
                           final int expectedDepthAfter) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Destination ingressExchangeDestination =
                    session.createQueue(getDestinationAddress(fromExchangeName, routingKey));

            assertEquals(String.format("Unexpected number of messages on queue '%s'", queueName),
                         expectedDepthBefore, getQueueDepth(queueName));

            Utils.sendMessages(connection, ingressExchangeDestination, 1);

            assertEquals(String.format("Unexpected number of messages on queue '%s", queueName),
                         expectedDepthAfter, getQueueDepth(queueName));
        }
        finally
        {
            connection.close();
        }
    }

    private String getDestinationAddress(final String exchangeName, final String routingKey)
    {
        return getProtocol() == Protocol.AMQP_1_0
                ? String.format("%s/%s", exchangeName, routingKey)
                : String.format("ADDR:%s/%s", exchangeName, routingKey);
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
