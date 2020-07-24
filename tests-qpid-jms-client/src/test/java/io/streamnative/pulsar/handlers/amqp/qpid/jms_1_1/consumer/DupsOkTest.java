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

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import org.junit.Test;

/**
 * DupsOkTest.
 */
public class DupsOkTest extends JmsTestBase
{

    @Test
    public void synchronousReceive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        final int numberOfMessages = 3;
        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            for (int i = 0; i < numberOfMessages; i++)
            {
                Message received = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Expected message (%d) not received", i), received);
                assertEquals("Unexpected message received", i, received.getIntProperty(INDEX));
            }

            assertNull("Received too many messages", consumer.receive(getReceiveTimeout()/4));

        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void asynchronousReceive() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        final int numberOfMessages = 3;
        try
        {
            connection.start();
            Utils.sendMessages(connection, queue, numberOfMessages);

            Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            AtomicReference<Throwable> exception = new AtomicReference<>();
            CountDownLatch completionLatch = new CountDownLatch(numberOfMessages);
            AtomicInteger expectedIndex = new AtomicInteger();

            consumer.setMessageListener(message -> {
                try
                {
                    Object index = message.getObjectProperty(INDEX);
                    assertEquals("Unexpected message received", expectedIndex.getAndIncrement(), message.getIntProperty(INDEX));
                }
                catch (Throwable e)
                {
                    exception.set(e);
                }
                finally
                {
                    completionLatch.countDown();
                }
            });

            boolean completed = completionLatch.await(getReceiveTimeout() * numberOfMessages, TimeUnit.MILLISECONDS);
            assertTrue("Message listener did not receive all messages within expected", completed);
            assertNull("Message listener encountered unexpected exception", exception.get());
        }
        finally
        {
            connection.close();
        }
    }

}
