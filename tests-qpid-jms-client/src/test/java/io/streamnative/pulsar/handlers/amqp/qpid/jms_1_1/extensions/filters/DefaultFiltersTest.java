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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.filters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Ignore;
import org.junit.Test;

/**
 * DefaultFiltersTest.
 */
@Ignore
public class DefaultFiltersTest extends JmsTestBase
{
    @Test
    public void defaultFilterIsApplied() throws Exception
    {
        String queueName = getTestName();
        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            createQueueWithDefaultFilter(queueName, "foo = 1");
            Queue queue = createQueue(queueName);

            final MessageProducer prod = session.createProducer(queue);
            Message message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            MessageConsumer cons = session.createConsumer(queue);

            assertNull("Message with foo=0 should not be received", cons.receive(getReceiveTimeout()));

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send(message);

            Message receivedMsg = cons.receive(getReceiveTimeout());
            assertNotNull("Message with foo=1 should be received", receivedMsg);
            assertEquals("Property foo not as expected", 1, receivedMsg.getIntProperty("foo"));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void defaultFilterIsOverridden() throws Exception
    {
        String queueName = getTestName();
        Connection connection = getConnection();
        try
        {
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            createQueueWithDefaultFilter(queueName, "foo = 1");
            Queue queue = createQueue(queueName);

            final MessageProducer prod = session.createProducer(queue);
            Message message = session.createMessage();
            message.setIntProperty("foo", 0);
            prod.send(message);

            MessageConsumer cons = session.createConsumer(queue, "foo = 0");

            Message receivedMsg = cons.receive(getReceiveTimeout());
            assertNotNull("Message with foo=0 should be received", receivedMsg);
            assertEquals("Property foo not as expected", 0, receivedMsg.getIntProperty("foo"));

            message = session.createMessage();
            message.setIntProperty("foo", 1);
            prod.send( message);

            assertNull("Message with foo=1 should not be received", cons.receive(getReceiveTimeout()));
        }
        finally
        {
            connection.close();
        }
    }

    private void createQueueWithDefaultFilter(String queueName, String selector) throws Exception
    {
        selector = selector.replace("\\", "\\\\");
        selector = selector.replace("\"", "\\\"");

        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("defaultFilters", "{ \"x-filter-jms-selector\" : { \"x-filter-jms-selector\" : [ \"" + selector + "\" ] } }"));
    }

}
