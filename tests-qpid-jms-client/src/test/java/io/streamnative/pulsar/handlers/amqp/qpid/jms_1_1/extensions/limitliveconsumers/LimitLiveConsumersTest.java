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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.limitliveconsumers;

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
import org.junit.Test;

/**
 * LimitLiveConsumersTest.
 */
public class LimitLiveConsumersTest extends JmsTestBase
{

    @Test
    public void testLimitLiveConsumers() throws Exception
    {
        String queueName = getTestName();
        createEntityUsingAmqpManagement(queueName, "org.apache.qpid.Queue",
                                        Collections.singletonMap("maximumLiveConsumers", 1));
        Queue queue = createQueue(queueName);
        int numberOfMessages = 5;
        Connection connection = getConnectionBuilder().setSyncPublish(true).build();
        try
        {
            Session session1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();


            MessageConsumer consumer1 = session1.createConsumer(queue);

            Session session2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer2 = session2.createConsumer(queue);


            Utils.sendMessages(session1, queue, numberOfMessages);


            for (int i = 0; i < 3; i++)
            {
                Message receivedMsg = consumer1.receive(getReceiveTimeout());
                assertNotNull("Message " + i + " not received", receivedMsg);
                assertEquals("Unexpected message", i, receivedMsg.getIntProperty(INDEX));
            }

            assertNull("Unexpected message arrived", consumer2.receive(getShortReceiveTimeout()));

            consumer1.close();
            session1.close();

            for (int i = 3; i < numberOfMessages; i++)
            {
                Message receivedMsg = consumer2.receive(getReceiveTimeout());
                assertNotNull("Message " + i + " not received", receivedMsg);
                assertEquals("Unexpected message", i, receivedMsg.getIntProperty(INDEX));
            }

            assertNull("Unexpected message arrived", consumer2.receive(getShortReceiveTimeout()));

            Session session3 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer3 = session3.createConsumer(queue);

            MessageProducer producer = session3.createProducer(queue);
            producer.send(Utils.createNextMessage(session3, 6));
            producer.send(Utils.createNextMessage(session3, 7));


            assertNotNull("Message not received on second consumer", consumer2.receive(getReceiveTimeout()));
            assertNull("Message unexpectedly received on third consumer", consumer3.receive(getShortReceiveTimeout()));

            Session session4 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer consumer4 = session4.createConsumer(queue);

            assertNull("Message unexpectedly received on fourth consumer", consumer4.receive(getShortReceiveTimeout()));
            consumer3.close();
            session3.close();

            assertNull("Message unexpectedly received on fourth consumer", consumer4.receive(getShortReceiveTimeout()));
            consumer2.close();
            session2.close();

            assertNotNull("Message not received on fourth consumer", consumer4.receive(getReceiveTimeout()));


            assertNull("Unexpected message arrived", consumer4.receive(getShortReceiveTimeout()));
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
