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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.queue;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.model.OverflowPolicy;

/**
 * OverflowPolicyTestBase.
 */
public class OverflowPolicyTestBase extends JmsTestBase
{
    private final byte[] BYTE_300 = new byte[300];


    protected Queue createQueueWithOverflowPolicy(final String queueName,
                                                  final OverflowPolicy overflowPolicy,
                                                  final int maxQueueDepthBytes,
                                                  final int maxQueueDepthMessages,
                                                  final int resumeCapacity) throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        if (maxQueueDepthBytes > 0)
        {
            attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_BYTES, maxQueueDepthBytes);
            if (resumeCapacity > 0)
            {
                String flowResumeLimit = getFlowResumeLimit(maxQueueDepthBytes, resumeCapacity);
                attributes.put(org.apache.qpid.server.model.Queue.CONTEXT,
                               String.format("{\"%s\": %s}",
                                             org.apache.qpid.server.model.Queue.QUEUE_FLOW_RESUME_LIMIT,
                                             flowResumeLimit));
            }
        }
        if (maxQueueDepthMessages > 0)
        {
            attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_QUEUE_DEPTH_MESSAGES, maxQueueDepthMessages);
        }
        attributes.put(org.apache.qpid.server.model.Queue.OVERFLOW_POLICY, overflowPolicy.name());
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue", attributes);
        return createQueue(queueName);
    }

    protected String getFlowResumeLimit(final double maximumCapacity, final double resumeCapacity)
    {
        double ratio = resumeCapacity / maximumCapacity;
        return String.format("%.2f", ratio * 100.0);
    }

    protected int evaluateMessageSize() throws Exception
    {
        String tmpQueueName = getTestName() + "_Tmp";
        Queue tmpQueue = createQueue(tmpQueueName);
        final Connection connection = getConnection();
        try
        {
            connection.start();
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer tmpQueueProducer = session.createProducer(tmpQueue);
            tmpQueueProducer.send(nextMessage(0, session));
            session.commit();
            return getQueueDepthBytes(tmpQueueName);
        }
        finally
        {
            connection.close();
        }
    }

    protected int getQueueDepthBytes(final String queueName) throws Exception
    {
        return getStatistics(queueName, "queueDepthBytes").intValue();
    }

    protected Number getStatistics(final String queueName, final String statisticsName) throws Exception
    {
        Map<String, Object> arguments =
                Collections.singletonMap("statistics", Collections.singletonList(statisticsName));
        Object statistics = performOperationUsingAmqpManagement(queueName,
                                                                "getStatistics",
                                                                "org.apache.qpid.Queue",
                                                                arguments);
        assertNotNull("Statistics is null", statistics);
        assertTrue("Statistics is not map", statistics instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> statisticsMap = (Map<String, Object>) statistics;
        assertTrue(String.format("%s is not present", statisticsName),
                   statisticsMap.get(statisticsName) instanceof Number);
        return ((Number) statisticsMap.get(statisticsName));
    }

    protected Message nextMessage(int index, Session producerSession) throws JMSException
    {
        BytesMessage send = producerSession.createBytesMessage();
        send.writeBytes(BYTE_300);
        send.setIntProperty(INDEX, index);
        return send;
    }
}
