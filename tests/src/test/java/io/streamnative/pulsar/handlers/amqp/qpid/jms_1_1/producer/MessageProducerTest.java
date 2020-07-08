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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.producer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.model.Protocol;
import org.junit.Test;

/**
 * MessageProducerTest.
 */
public class MessageProducerTest extends JmsTestBase
{
    @Test
    public void produceToUnknownQueue() throws Exception
    {
        assumeThat("QPID-7818", getProtocol(), is(not(equalTo(Protocol.AMQP_0_10))));

        Connection connection = getConnectionBuilder().setSyncPublish(true).build();

        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue invalidDestination = session.createQueue("unknown");

            try
            {
                MessageProducer sender = session.createProducer(invalidDestination);
                sender.send(session.createMessage());
                fail("Exception not thrown");
            }
            catch (InvalidDestinationException e)
            {
                //PASS
            }
            catch (JMSException e)
            {
                assertThat("Allowed for the Qpid JMS AMQP 0-x client",
                           getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
                //PASS
            }
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void anonymousSenderSendToUnknownQueue() throws Exception
    {
        assumeThat("QPID-7818", getProtocol(), is(not(equalTo(Protocol.AMQP_0_10))));

        Connection connection = getConnectionBuilder().setSyncPublish(true).build();

        try
        {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue invalidDestination = session.createQueue("unknown");

            try
            {
                MessageProducer sender = session.createProducer(null);
                sender.send(invalidDestination, session.createMessage());
                fail("Exception not thrown");
            }
            catch (InvalidDestinationException e)
            {
                //PASS
            }
            catch (JMSException e)
            {
                assertThat("Allowed for the Qpid JMS AMQP 0-x client",
                           getProtocol(), is(not(equalTo(Protocol.AMQP_1_0))));
                //PASS
            }

        }
        finally
        {
            connection.close();
        }
    }

}
