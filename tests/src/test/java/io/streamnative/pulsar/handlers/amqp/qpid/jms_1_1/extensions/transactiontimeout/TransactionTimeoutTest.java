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
package io.streamnative.pulsar.handlers.amqp.qpid.jms_1_1.extensions.transactiontimeout;

import static org.apache.qpid.server.virtualhost.QueueManagingVirtualHost.STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE;
import static org.apache.qpid.server.virtualhost.QueueManagingVirtualHost.STORE_TRANSACTION_OPEN_TIMEOUT_CLOSE;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import com.google.common.util.concurrent.SettableFuture;
import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.qpid.server.model.Protocol;
import org.junit.Before;
import org.junit.Test;

/**
 * TransactionTimeoutTest.
 */
public class TransactionTimeoutTest extends JmsTestBase
{
    private static final long CLOSE_TIME = 500L;

    private final ExceptionCatchingListener _listener = new ExceptionCatchingListener();

    @Before
    public void setUp()
    {
        assumeThat(getBrokerAdmin().isManagementSupported(), is(true));
    }

    @Test
    public void producerTransactionIdle() throws Exception
    {
        enableTransactionTimeout(Collections.singletonMap(STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE, CLOSE_TIME));

        final Queue queue = createQueue(getTestName());
        final Connection connection = getConnection();
        try
        {
            connection.setExceptionListener(_listener);
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            _listener.assertNoException(CLOSE_TIME * 2, TimeUnit.MILLISECONDS);

            producer.send(session.createMessage());

            _listener.assertConnectionExceptionReported(CLOSE_TIME * 2, TimeUnit.MILLISECONDS);

            try
            {
                session.commit();
                fail("Exception not thrown");
            }
            catch (JMSException e)
            {
                // PASS
            }
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (JMSException ignore)
            {
            }
        }
    }

    @Test
    public void producerTransactionOpen() throws Exception
    {
        enableTransactionTimeout(Collections.singletonMap(STORE_TRANSACTION_OPEN_TIMEOUT_CLOSE, CLOSE_TIME));

        final Queue queue = createQueue(getTestName());
        final Connection connection = getConnection();
        try
        {
            connection.setExceptionListener(_listener);
            Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
            MessageProducer producer = session.createProducer(queue);

            final long halfTime = System.currentTimeMillis() + CLOSE_TIME / 2;
            while (halfTime > System.currentTimeMillis())
            {
                producer.send(session.createMessage());
                Thread.sleep(25);
            }

            final long fullTime = System.currentTimeMillis() + CLOSE_TIME;
            boolean exceptionReceived = false;
            while (!(fullTime <= System.currentTimeMillis() && exceptionReceived))
            {
                try
                {
                    producer.send(session.createMessage());
                    Thread.sleep(25);
                }
                catch (JMSException e)
                {
                    exceptionReceived = true;
                }
            }
            assertThat("Transaction open for an excessive length of time was not closed",
                       exceptionReceived, is(equalTo(true)));

            _listener.assertConnectionExceptionReported(CLOSE_TIME, TimeUnit.MILLISECONDS);

            try
            {
                session.commit();
                fail("Exception not thrown");
            }
            catch (JMSException e)
            {
                // PASS
            }
        }
        finally
        {
            try
            {
                connection.close();
            }
            catch (JMSException ignore)
            {
            }
        }
    }

    private void enableTransactionTimeout(final Map<String, Object> attrs) throws Exception
    {
        Connection connection = getConnection();
        try
        {
            final Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

            final Queue queue;
            if(getProtocol() == Protocol.AMQP_1_0)
            {
                queue = session.createQueue("$management");
            }
            else
            {
                queue = session.createQueue("ADDR:$management");
            }

            final MessageProducer _producer = session.createProducer(queue);
            MapMessage message = session.createMapMessage();

            message.setStringProperty("type", "org.apache.qpid.VirtualHost");
            message.setStringProperty("operation", "UPDATE");
            message.setStringProperty("index", "object-path");
            message.setStringProperty("key", "");

            for (final Map.Entry<String, Object> entry : attrs.entrySet())
            {
                message.setObject(entry.getKey(), entry.getValue());
            }

            _producer.send(message);
            session.commit();
        }
        finally
        {
            connection.close();
        }
    }

    private static class ExceptionCatchingListener implements ExceptionListener
    {
        private final SettableFuture<JMSException> _future = SettableFuture.create();

        @Override
        public void onException(final JMSException exception)
        {
            _future.set(exception);
        }

        void assertConnectionExceptionReported(final long time, final TimeUnit timeUnit) throws Exception
        {
            final JMSException jmsException = _future.get(time, timeUnit);
            assertThat(jmsException.getMessage(), containsString("transaction timed out"));
        }

        void assertNoException(final long time, final TimeUnit timeUnit) throws Exception
        {
            try
            {
                _future.get(time, timeUnit);
                assertThat("Exception unexpectedly received by listener", _future.isDone(), is(equalTo(true)));
            }
            catch (TimeoutException e)
            {
                // PASS
            }
        }
    }
}
