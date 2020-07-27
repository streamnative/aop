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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.consumer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.time.Instant;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Ignore;
import org.junit.Test;

/**
 * SynchReceiveTest.
 */
public class SynchReceiveTest extends JmsTestBase
{

    @Test
    public void receiveWithTimeout() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();

        try
        {
            final int numberOfMessages = 3;
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int num = 0; num < numberOfMessages; num++)
            {
                assertNotNull(String.format("Expected message (%d) not received", num), consumer.receive(getReceiveTimeout()));
            }

            assertNull("Received too many messages", consumer.receive(getReceiveTimeout()));

        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void receiveNoWait() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();

        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final Instant timeout = Instant.now().plusMillis(getReceiveTimeout());
            Message msg;
            do
            {
                msg = consumer.receiveNoWait();
            }
            while(msg == null && Instant.now().isBefore(timeout));

            assertNotNull("Expected message not received within timeout", msg);
            assertNull("Received too many messages", consumer.receiveNoWait());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    @Ignore
    public void twoConsumersInterleaved() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnectionBuilder().setPrefetch(0).build();

        try
        {
            final int numberOfReceiveLoops = 3;
            final int numberOfMessages = numberOfReceiveLoops * 2;

            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session consumerSession1 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer1 = consumerSession1.createConsumer(queue);

            Session consumerSession2 = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer2 = consumerSession2.createConsumer(queue);

            for(int i = 0; i < numberOfReceiveLoops; i++)
            {
                final Message receive1 = consumer1.receive(getReceiveTimeout());
                assertNotNull("Expected message not received from consumer1 within timeout", receive1);

                final Message receive2 = consumer2.receive(getReceiveTimeout());
                assertNotNull("Expected message not received from consumer1 within timeout", receive2);
            }
        }
        finally
        {
            connection.close();
        }
    }
}
