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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.nondestructiveconsumer;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
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
 * EnsureNondestructiveConsumersTest.
 */
@Ignore
public class EnsureNondestructiveConsumersTest extends JmsTestBase
{

    @Test
    public void testEnsureNondestructiveConsumers() throws Exception
    {
        String queueName = getTestName();
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("ensureNondestructiveConsumers", true));
        Queue queue = createQueue(queueName);
        int numberOfMessages = 5;
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();

            Utils.sendMessages(session, queue, numberOfMessages);

            MessageConsumer consumer1 = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Message receivedMsg = consumer1.receive(getReceiveTimeout());
                assertNotNull("Message " + i + " not received", receivedMsg);
                assertEquals("Unexpected message", i, receivedMsg.getIntProperty(INDEX));
            }

            assertNull("Unexpected message arrived", consumer1.receive(getShortReceiveTimeout()));

            MessageConsumer consumer2 = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Message receivedMsg = consumer2.receive(getReceiveTimeout());
                assertNotNull("Message " + i + " not received", receivedMsg);
                assertEquals("Unexpected message", i, receivedMsg.getIntProperty(INDEX));
            }

            assertNull("Unexpected message arrived", consumer2.receive(getShortReceiveTimeout()));

            MessageProducer producer = session.createProducer(queue);
            producer.send(Utils.createNextMessage(session, 6));

            assertNotNull("Message not received on first consumer", consumer1.receive(getReceiveTimeout()));
            assertNotNull("Message not received on second consumer", consumer2.receive(getReceiveTimeout()));

            assertNull("Unexpected message arrived", consumer1.receive(getShortReceiveTimeout()));
            assertNull("Unexpected message arrived", consumer2.receive(getShortReceiveTimeout()));
        }
        finally
        {
            connection.close();
        }
    }

    private long getShortReceiveTimeout()
    {
        return getReceiveTimeout() / 4;
    }
}
