


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

import static org.junit.Assert.assertNull;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.QueueingConsumer;
import java.io.IOException;
import org.junit.Test;



/**
 * Reject.
 */
public class Reject extends AbstractRejectTest {

    @Test
    public void reject()
        throws IOException, InterruptedException {
        String exchange = generateExchangeName();
        String queue = generateQueueName();
        String routingKey = "key-1";
        declareExchangeAndQueueToBind(queue, exchange, routingKey);
        byte[] m1 = "1".getBytes();
        byte[] m2 = "2".getBytes();
        basicPublishPersistent(m1, exchange, routingKey);
        basicPublishPersistent(m2, exchange, routingKey);

        long tag1 = checkDelivery(channel.basicGet(queue, false), m1, false);
        long tag2 = checkDelivery(channel.basicGet(queue, false), m2, false);
        QueueingConsumer c = new QueueingConsumer(secondaryChannel);
        String consumerTag = secondaryChannel.basicConsume(queue, false, c);
        channel.basicReject(tag2, true);
        long tag3 = checkDelivery(c.nextDelivery(), m2, true);
        secondaryChannel.basicCancel(consumerTag);
        secondaryChannel.basicReject(tag3, false);
        assertNull(channel.basicGet(queue, false));
        channel.basicAck(tag1, false);
        channel.basicReject(tag3, false);
        expectError(AMQP.PRECONDITION_FAILED);
    }
}
