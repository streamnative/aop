
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import com.rabbitmq.client.test.BrokerTestCase;
import org.junit.Test;

/**
 * Testcase.
 */
public class Heartbeat extends BrokerTestCase {

    @Override
    protected ConnectionFactory newConnectionFactory() {
        ConnectionFactory cf = super.newConnectionFactory();
        cf.setRequestedHeartbeat(1);
        return cf;
    }

    @Test
    public void heartbeat() throws InterruptedException {
        assertEquals(1, connection.getHeartbeat());
        Thread.sleep(3100);
        assertTrue(connection.isOpen());
        ((AutorecoveringConnection) connection).getDelegate().setHeartbeat(0);
        assertEquals(0, connection.getHeartbeat());
        Thread.sleep(3100);
        assertFalse(connection.isOpen());

    }

}
