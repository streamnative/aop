

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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;

/**
 * See bug 21846.
 * Basic.Ack is now required to signal a channel error immediately upon
 * detecting an invalid deliveryTag, even if the channel is (Tx-)transacted.
 * Specifically, a client MUST not acknowledge the same message more than once.
 */
public abstract class InvalidAcksBase extends BrokerTestCase {
    protected abstract void select() throws IOException;

    protected abstract void commit() throws IOException;

    //@Test
    public void doubleAck()
            throws IOException {
        select();
        String q = channel.queueDeclare().getQueue();
        basicPublishVolatile(q);
        commit();

        long tag = channel.basicGet(q, false).getEnvelope().getDeliveryTag();
        channel.basicAck(tag, false);
        channel.basicAck(tag, false);

        expectError(AMQP.PRECONDITION_FAILED);
    }

    //@Test
    public void crazyAck()
            throws IOException {
        select();
        channel.basicAck(123456, false);
        expectError(AMQP.PRECONDITION_FAILED);
    }
}
