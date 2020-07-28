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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.autocreation;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;
import org.apache.qpid.server.virtualhost.QueueManagingVirtualHost;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * DefaultAlternateBindingTest.
 */
public class DefaultAlternateBindingTest extends JmsTestBase
{
    // quadruple of '$' is used intentionally instead of '$$' to account conversion in management layer
    private static final String DEFAULT_ALTERNATE_BINDING = "{\"destination\": \"$$$${this:name}_DLQ\"}";
    private static final int MAXIMUM_DELIVERY_ATTEMPTS = 1;
    private static final int MESSAGE_COUNT = 4;
    private String _queueName;

    @Before
    public void setUp() throws Exception
    {
        _queueName = getTestName();
        updateVirtualHostForDefaultAlternateBinding();
    }

    @Test
    @Ignore
    public void testDefaultAlternateBinding() throws Exception
    {
        createAndBindQueueWithMaxDeliveryAttempts();

        final Connection connection =
                getConnectionBuilder().setMessageRedelivery(true).setSyncPublish(true).setPrefetch(0).build();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            final Queue queue = session.createQueue(_queueName);
            Utils.sendMessages(session, queue, MESSAGE_COUNT);

            connection.start();
            final MessageConsumer queueConsumer = session.createConsumer(queue);

            for (int i = 0; i < MESSAGE_COUNT; i++)
            {
                final Message message = queueConsumer.receive(getReceiveTimeout());
                assertThat(message, is(notNullValue()));
                int index = message.getIntProperty(Utils.INDEX);
                if (index % 2 == 0)
                {
                    session.commit();
                }
                else
                {
                    session.rollback();
                }
            }

            final Queue dlq = session.createQueue(_queueName + "_DLQ");
            final MessageConsumer dlqConsumer = session.createConsumer(dlq);

            final Message message2 = dlqConsumer.receive(getReceiveTimeout());
            assertThat(message2, is(notNullValue()));
            assertThat(message2.getIntProperty(Utils.INDEX), is(equalTo(1)));

            final Message message4 = dlqConsumer.receive(getReceiveTimeout());
            assertThat(message4, is(notNullValue()));
            assertThat(message4.getIntProperty(Utils.INDEX), is(equalTo(3)));
        }
        finally
        {
            connection.close();
        }
    }

    private void createAndBindQueueWithMaxDeliveryAttempts() throws Exception
    {
        final Map<String, Object> queueAttributes =
                Collections.singletonMap(org.apache.qpid.server.model.Queue.MAXIMUM_DELIVERY_ATTEMPTS,
                                         MAXIMUM_DELIVERY_ATTEMPTS);
        createEntityUsingAmqpManagement(_queueName, "org.apache.qpid.Queue", queueAttributes);
        final Map<String, Object> arguments = new HashMap<>();
        arguments.put("destination", _queueName);
        arguments.put("bindingKey", _queueName);
        performOperationUsingAmqpManagement("amq.direct", "bind", "org.apache.qpid.Exchange", arguments);
    }


    private void updateVirtualHostForDefaultAlternateBinding() throws Exception
    {
        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(QueueManagingVirtualHost.CONTEXT,
                       objectToJsonString(Collections.singletonMap("queue.defaultAlternateBinding",
                                                                   DEFAULT_ALTERNATE_BINDING)));
        attributes.put(QueueManagingVirtualHost.NODE_AUTO_CREATION_POLICIES, createAutoCreationPolicies());
        updateEntityUsingAmqpManagement(getVirtualHostName(), "org.apache.qpid.VirtualHost", attributes);
    }

    private String createAutoCreationPolicies() throws JsonProcessingException
    {
        List<NodeAutoCreationPolicy> objects = Collections.singletonList(new NodeAutoCreationPolicy()
        {
            @Override
            public String getPattern()
            {
                return ".*_DLQ";
            }

            @Override
            public boolean isCreatedOnPublish()
            {
                return true;
            }

            @Override
            public boolean isCreatedOnConsume()
            {
                return false;
            }

            @Override
            public String getNodeType()
            {
                return "Queue";
            }

            @Override
            public Map<String, Object> getAttributes()
            {
                return Collections.singletonMap("alternateBinding", "");
            }
        });
        return objectToJsonString(objects);
    }

    private String objectToJsonString(final Object objects) throws JsonProcessingException
    {
        return new ObjectMapper().writeValueAsString(objects);
    }
}
