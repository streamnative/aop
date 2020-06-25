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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.QueueingConsumer;
import java.util.Arrays;
import org.junit.After;
import org.junit.Before;



/**
 * Testcase.
 */

public abstract class AbstractRejectTest extends BrokerTestCase {

    protected Channel secondaryChannel;

    @Override
    @Before
    public void setUp()
        throws Exception {
        super.setUp();
        secondaryChannel = connection.createChannel();

    }

    @Override
    @After
    public void cleanup()
        throws Exception {
        if (secondaryChannel != null) {
            secondaryChannel.abort();
            secondaryChannel = null;
        }
        super.cleanup();
    }

    protected long checkDelivery(QueueingConsumer.Delivery d,
        byte[] msg, boolean redelivered) {
        assertNotNull(d);
        return checkDelivery(d.getEnvelope(), d.getBody(), msg, redelivered);
    }

    protected long checkDelivery(GetResponse r, byte[] msg, boolean redelivered) {
        assertNotNull(r);
        return checkDelivery(r.getEnvelope(), r.getBody(), msg, redelivered);
    }

    protected long checkDelivery(Envelope e, byte[] m,
        byte[] msg, boolean redelivered) {
        assertNotNull(e);
        assertTrue(Arrays.equals(m, msg));
        assertEquals(e.isRedeliver(), redelivered);
        return e.getDeliveryTag();
    }
}
