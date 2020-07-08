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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

import io.streamnative.pulsar.handlers.amqp.qpid.core.JmsTestBase;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import org.junit.Test;

/**
 * ExceptionListenerTest.
 */
public class ExceptionListenerTest extends JmsTestBase
{
    @Test
    public void testExceptionListenerHearsBrokerShutdown() throws Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(equalTo(true)));

        final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
        final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
        Connection connection = getConnection();
        try
        {
            connection.setExceptionListener(exception -> {
                exceptionHolder.set(exception);
                exceptionReceivedLatch.countDown();
            });

            getBrokerAdmin().restart();

            assertTrue("Exception was not propagated into exception listener in timely manner",
                       exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNotNull("Unexpected exception", exceptionHolder.get());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExceptionListenerClosesConnectionIsAllowed() throws  Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(equalTo(true)));

        final Connection connection = getConnection();
        try
        {
            final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
            final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
            final AtomicReference<Throwable> unexpectedExceptionHolder = new AtomicReference<>();
            final ExceptionListener listener = exception -> {
                exceptionHolder.set(exception);
                try
                {
                    connection.close();
                    // PASS
                }
                catch (Throwable t)
                {
                    unexpectedExceptionHolder.set(t);
                }
                finally
                {
                    exceptionReceivedLatch.countDown();
                }
            };
            connection.setExceptionListener(listener);

            getBrokerAdmin().restart();

            assertTrue("Exception was not propagated into exception listener in timely manner",
                       exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNotNull("Unexpected exception", exceptionHolder.get());
            assertNull("Connection#close() should not have thrown exception", unexpectedExceptionHolder.get());
        }
        finally
        {
            connection.close();
        }
    }

    @Test
    public void testExceptionListenerStopsConnection_ThrowsIllegalStateException() throws  Exception
    {
        assumeThat(getBrokerAdmin().supportsRestart(), is(equalTo(true)));

        final Connection connection = getConnection();
        try
        {
            final CountDownLatch exceptionReceivedLatch = new CountDownLatch(1);
            final AtomicReference<JMSException> exceptionHolder = new AtomicReference<>();
            final AtomicReference<Throwable> unexpectedExceptionHolder = new AtomicReference<>();
            final ExceptionListener listener = exception -> {
                exceptionHolder.set(exception);
                try
                {
                    connection.stop();
                    fail("Exception not thrown");
                }
                catch (IllegalStateException ise)
                {
                    // PASS
                }
                catch (Throwable t)
                {
                    unexpectedExceptionHolder.set(t);
                }
                finally
                {
                    exceptionReceivedLatch.countDown();
                }
            };
            connection.setExceptionListener(listener);

            getBrokerAdmin().restart();

            assertTrue("Exception was not propagated into exception listener in timely manner",
                       exceptionReceivedLatch.await(getReceiveTimeout(), TimeUnit.MILLISECONDS));
            assertNotNull("Unexpected exception", exceptionHolder.get());
            assertNull("Connection#stop() should not have thrown exception", unexpectedExceptionHolder.get());
        }
        finally
        {
            connection.close();
        }
    }

}
