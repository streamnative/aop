

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

import static com.rabbitmq.client.impl.recovery.TopologyRecoveryRetryLogic.RETRY_ON_QUEUE_NOT_FOUND_RETRY_HANDLER;
import static com.rabbitmq.client.test.TestUtils.closeAllConnectionsAndWaitForRecovery;
import static org.junit.Assert.assertTrue;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.TestUtils;
import java.util.HashMap;
import org.junit.Test;

/**
 *
 */
public class TopologyRecoveryRetry extends BrokerTestCase {

    @Test
    public void topologyRecoveryRetry() throws Exception {
        int nbQueues = 200;
        String prefix = "topology-recovery-retry-" + System.currentTimeMillis();
        for (int i = 0; i < nbQueues; i++) {
            String queue = prefix + i;
            channel.queueDeclare(queue, false, false, true, new HashMap<>());
            channel.queueBind(queue, "amq.direct", queue);
            channel.basicConsume(queue, true, new DefaultConsumer(channel));
        }

        closeAllConnectionsAndWaitForRecovery(this.connection);

        assertTrue(channel.isOpen());
    }

    @Override
    protected ConnectionFactory newConnectionFactory() {
        ConnectionFactory connectionFactory = TestUtils.connectionFactory();
        connectionFactory.setTopologyRecoveryRetryHandler(RETRY_ON_QUEUE_NOT_FOUND_RETRY_HANDLER.build());
        connectionFactory.setNetworkRecoveryInterval(1000);
        return connectionFactory;
    }
}
