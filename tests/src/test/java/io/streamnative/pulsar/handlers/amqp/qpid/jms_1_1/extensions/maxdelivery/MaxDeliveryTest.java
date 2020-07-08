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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.maxdelivery;

import static io.streamnative.pulsar.handlers.amqp.qpid.core.Utils.INDEX;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import io.streamnative.pulsar.handlers.amqp.qpid.core.Utils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import org.apache.qpid.server.model.AlternateBinding;
import org.junit.Test;

/**
 * MaxDeliveryTest.
 */
public class MaxDeliveryTest extends JmsTestBase
{
    private static final int MAX_DELIVERY_ATTEMPTS = 2;
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    @Test
    public void maximumDelivery() throws Exception
    {
        String queueName = getTestName();
        String dlqNAme = getTestName() + "_DLQ";

        createMaxDeliveryQueueAndDLQ(queueName, MAX_DELIVERY_ATTEMPTS, dlqNAme);
        int numberOfMessages = 5;
        Connection connection = getConnectionBuilder().setMessageRedelivery(true).build();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Queue queue = session.createQueue(queueName);
            final MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, numberOfMessages);

            connection.start();

            int expectedMessageIndex = 0;
            int deliveryAttempt = 0;
            int deliveryCounter = 0;
            do
            {
                Message message = consumer.receive(getReceiveTimeout());
                assertNotNull(String.format("Message '%d' was not received in delivery attempt %d",
                                            expectedMessageIndex,
                                            deliveryAttempt), message);
                int index = message.getIntProperty(INDEX);
                assertEquals(String.format("Unexpected message index (delivery attempt %d)", deliveryAttempt),
                             expectedMessageIndex,
                             index);

                deliveryCounter++;

                // dlq all even messages
                if (index % 2 == 0)
                {
                    session.rollback();
                    if (deliveryAttempt < MAX_DELIVERY_ATTEMPTS - 1)
                    {
                        deliveryAttempt++;
                    }
                    else
                    {
                        deliveryAttempt = 0;
                        expectedMessageIndex++;
                    }
                }
                else
                {
                    session.commit();
                    deliveryAttempt = 0;
                    expectedMessageIndex++;
                }
            }
            while (expectedMessageIndex != numberOfMessages);

            int numberOfEvenMessages = numberOfMessages / 2 + 1;
            assertEquals("Unexpected total delivery counter",
                         numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages),
                         deliveryCounter);

            verifyDeadLetterQueueMessages(connection, dlqNAme, numberOfEvenMessages);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void maximumDeliveryWithinMessageListener() throws Exception
    {
        String queueName = getTestName();
        String dlqName = getTestName() + "_DLQ";

        createMaxDeliveryQueueAndDLQ(queueName, MAX_DELIVERY_ATTEMPTS, dlqName);
        int numberOfMessages = 5;
        Connection connection = getConnectionBuilder().setMessageRedelivery(true).build();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Queue queue = session.createQueue(queueName);
            final MessageConsumer consumer = session.createConsumer(queue);
            Utils.sendMessages(connection, queue, numberOfMessages);

            connection.start();

            int numberOfEvenMessages = numberOfMessages / 2 + 1;
            int expectedNumberOfDeliveries =
                    numberOfEvenMessages * MAX_DELIVERY_ATTEMPTS + (numberOfMessages - numberOfEvenMessages);
            final CountDownLatch deliveryLatch = new CountDownLatch(expectedNumberOfDeliveries);
            final AtomicReference<Throwable> messageListenerThrowable = new AtomicReference<>();
            final AtomicInteger deliveryAttempt = new AtomicInteger();
            final AtomicInteger expectedMessageIndex = new AtomicInteger();
            consumer.setMessageListener(message -> {
                try
                {
                    int index = message.getIntProperty(INDEX);
                    assertEquals(String.format("Unexpected message index (delivery attempt %d)", deliveryAttempt.get()),
                                 expectedMessageIndex.get(),
                                 index);

                    // dlq all even messages
                    if (index % 2 == 0)
                    {
                        session.rollback();
                        if (deliveryAttempt.get() < MAX_DELIVERY_ATTEMPTS - 1)
                        {
                            deliveryAttempt.incrementAndGet();
                        }
                        else
                        {
                            deliveryAttempt.set(0);
                            expectedMessageIndex.incrementAndGet();
                        }
                    }
                    else
                    {
                        session.commit();
                        deliveryAttempt.set(0);
                        expectedMessageIndex.incrementAndGet();
                    }
                }
                catch (Throwable t)
                {
                    messageListenerThrowable.set(t);
                }
                finally
                {
                    deliveryLatch.countDown();
                }
            });

            assertTrue("Messages were not received in timely manner",
                       deliveryLatch.await(expectedNumberOfDeliveries * getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNull("Unexpected throwable in MessageListener", messageListenerThrowable.get());

            verifyDeadLetterQueueMessages(connection, dlqName, numberOfEvenMessages);
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void browsingDoesNotIncrementDeliveryCount() throws Exception
    {
        assumeThat(getBrokerAdmin().isManagementSupported(), is(true));

        final String queueName = getTestName();
        getBrokerAdmin().createQueue(queueName);

        Connection connection = getConnection();
        try
        {
            connection.start();
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            final Queue queue = session.createQueue(queueName);

            Utils.sendMessages(connection, queue, 1);

            final Map<String, Object> messageInfoBefore = getMessageInfo(queueName, 0);
            assertThat("Unexpected delivery count before browse", messageInfoBefore.get("deliveryCount"), is(equalTo(0)));

            browseQueueAndValidationDeliveryHeaders(session, queue);

            final Map<String, Object> messageInfoAfter = getMessageInfo(queueName, 0);
            assertThat("Unexpected delivery count after first browse", messageInfoAfter.get("deliveryCount"), is(equalTo(0)));

            browseQueueAndValidationDeliveryHeaders(session, queue);

        }
        finally
        {
            connection.close();
        }
    }

    private void verifyDeadLetterQueueMessages(final Connection connection,
                                               final String dlqName,
                                               final int numberOfEvenMessages) throws Exception
    {
        assertEquals("Unexpected number of total messages",
                     numberOfEvenMessages,
                     getTotalDepthOfQueuesMessages());

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue(dlqName);
        MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < numberOfEvenMessages; i++)
        {
            Message message = consumer.receive(getReceiveTimeout());
            assertEquals("Unexpected DLQ message index", i * 2, message.getIntProperty(INDEX));
        }
    }

    private void createMaxDeliveryQueueAndDLQ(String queueName, int maxDeliveryAttempts, String dlqName)
            throws Exception
    {
        createEntityUsingAmqpManagement(dlqName,
                                        "org.apache.qpid.StandardQueue",
                                        Collections.emptyMap());

        final Map<String, Object> attributes = new HashMap<>();
        attributes.put(org.apache.qpid.server.model.Queue.NAME, queueName);
        attributes.put(org.apache.qpid.server.model.Queue.MAXIMUM_DELIVERY_ATTEMPTS, maxDeliveryAttempts);
        attributes.put(org.apache.qpid.server.model.Queue.ALTERNATE_BINDING,
                       new ObjectMapper().writeValueAsString(Collections.singletonMap(AlternateBinding.DESTINATION,
                                                                                      dlqName)));

        createEntityUsingAmqpManagement(queueName,
                                        "org.apache.qpid.StandardQueue",
                                        attributes);
    }

    private Map<String, Object> getMessageInfo(String queueName, final int index) throws Exception
    {
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> messages = (List<Map<String, Object>>) performOperationUsingAmqpManagement(queueName,
                                                                                                             "getMessageInfo",
                                                                                                             "org.apache.qpid.Queue",
                                                                                                             Collections.emptyMap());
        assertThat("Too few messsages on the queue", messages.size(), is(greaterThan(index)));
        return messages.get(index);
    }

    private void browseQueueAndValidationDeliveryHeaders(final Session session, final Queue queue) throws Exception
    {
        final QueueBrowser browser = session.createBrowser(queue);
        @SuppressWarnings("unchecked")
        final List<Message> messages = (List<Message>) new ArrayList(Collections.list(browser.getEnumeration()));
        assertThat("Unexpected number of messages seen by browser", messages.size(), is(equalTo(1)));
        final Message browsedMessage = messages.get(0);
        assertThat(browsedMessage.getJMSRedelivered(), is(equalTo(false)));

        if (browsedMessage.propertyExists(JMSX_DELIVERY_COUNT))
        {
            assertThat(browsedMessage.getIntProperty(JMSX_DELIVERY_COUNT), is(equalTo(1)));

        }
        browser.close();
    }
}
