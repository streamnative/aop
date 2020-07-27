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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Ignore;
import org.junit.Test;

/**
 * ConnectionStartTest.
 */
public class ConnectionStartTest extends JmsTestBase
{
    @Test
    @Ignore
    public void testConsumerCanReceiveMessageAfterConnectionStart() throws Exception
    {
        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);
            assertNull("No messages should be delivered when the connection is stopped",
                       consumer.receive(getReceiveTimeout() / 2));
            connection.start();
            assertNotNull("There should be messages waiting for the consumer", consumer.receive(getReceiveTimeout()));
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testMessageListenerCanReceiveMessageAfterConnectionStart() throws Exception
    {

        Queue queue = createQueue(getTestName());
        Connection connection = getConnection();
        try
        {
            Utils.sendMessages(connection, queue, 1);

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(queue);

            final CountDownLatch awaitMessage = new CountDownLatch(1);
            final AtomicLong deliveryTime = new AtomicLong();
            consumer.setMessageListener(message -> {
                try
                {
                    deliveryTime.set(System.currentTimeMillis());
                }
                finally
                {
                    awaitMessage.countDown();
                }
            });

            long beforeStartTime = System.currentTimeMillis();
            connection.start();

            assertTrue("Message is not received in timely manner", awaitMessage.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertTrue("Message received before connection start", deliveryTime.get() >= beforeStartTime);
        }
        finally
        {
            connection.close();
        }

    }

}
