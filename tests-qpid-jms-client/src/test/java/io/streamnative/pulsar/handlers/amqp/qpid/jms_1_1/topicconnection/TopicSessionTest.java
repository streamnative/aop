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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.topicconnection;

import static org.junit.Assert.fail;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import org.junit.Test;

/**
 * TopicSessionTest.
 */
public class TopicSessionTest extends JmsTestBase
{
    @Test
    public void testTopicSessionCannotCreateCreateBrowser() throws Exception
    {
        Queue queue = createQueue(getTestName());
        TopicConnection topicConnection = getTopicConnection();
        try
        {
            TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            topicSession.createBrowser(queue);
            fail("Expected exception was not thrown");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // PASS
        }
        finally
        {
            topicConnection.close();
        }
    }

    @Test
    public void testTopicSessionCannotCreateQueues() throws Exception
    {
        TopicConnection topicConnection = getTopicConnection();
        try
        {
            TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            topicSession.createQueue("abc");
            fail("Expected exception was not thrown");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // PASS
        }
        finally
        {
            topicConnection.close();
        }
    }

    @Test
    public void testTopicSessionCannotCreateTemporaryQueues() throws Exception
    {
        TopicConnection topicConnection = getTopicConnection();
        try
        {
            TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            topicSession.createTemporaryQueue();
            fail("Expected exception was not thrown");
        }
        catch (javax.jms.IllegalStateException s)
        {
            // PASS
        }
        finally
        {
            topicConnection.close();
        }
    }

    @Test
    public void publisherGetDeliveryModeAfterConnectionClose() throws Exception
    {
        Topic topic = createTopic(getTestName());
        TopicConnection connection =  getTopicConnection();
        try
        {
            TopicSession session = connection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
            TopicPublisher publisher = session.createPublisher(topic);
            connection.close();
            try
            {
                publisher.getDeliveryMode();
                fail("Expected exception not thrown");
            }
            catch (javax.jms.IllegalStateException e)
            {
                // PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

}
