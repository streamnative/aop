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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.queueconnection;

import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Topic;
import org.junit.Test;

/**
 * QueueSessionTest.
 */
public class QueueSessionTest extends JmsTestBase
{

    @Test
    public void testQueueSessionCannotCreateTemporaryTopics() throws Exception
    {
        QueueConnection queueConnection = getQueueConnection();
        try
        {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                queueSession.createTemporaryTopic();
                fail("expected exception did not occur");
            }
            catch (javax.jms.IllegalStateException s)
            {
                // PASS
            }
        }
        finally
        {
            queueConnection.close();
        }
    }

    @Test
    public void testQueueSessionCannotCreateTopics() throws Exception
    {
        QueueConnection queueConnection = getQueueConnection();
        try
        {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                queueSession.createTopic("abc");
                fail("expected exception did not occur");
            }
            catch (javax.jms.IllegalStateException s)
            {
                // PASS
            }
        }
        finally
        {
            queueConnection.close();
        }
    }

    @Test
    public void testQueueSessionCannotCreateDurableSubscriber() throws Exception
    {
        Topic topic = createTopic(getTestName());
        QueueConnection queueConnection = getQueueConnection();
        try
        {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

            try
            {
                queueSession.createDurableSubscriber(topic, "abc");
                fail("expected exception did not occur");
            }
            catch (javax.jms.IllegalStateException s)
            {
                // PASS
            }
        }
        finally
        {
            queueConnection.close();
        }
    }

    @Test
    public void testQueueSessionCannotUnsubscribe() throws Exception
    {
        QueueConnection queueConnection = getQueueConnection();
        try
        {
            QueueSession queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
            try
            {
                queueSession.unsubscribe("abc");
                fail("expected exception did not occur");
            }
            catch (javax.jms.IllegalStateException s)
            {
                // PASS
            }
        }
        finally
        {
            queueConnection.close();
        }
    }
}
