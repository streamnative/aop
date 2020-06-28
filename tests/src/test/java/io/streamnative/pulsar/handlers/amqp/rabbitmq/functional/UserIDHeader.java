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

import static org.junit.Assert.fail;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.test.BrokerTestCase;
import com.rabbitmq.client.test.Host;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * UserIDHeader.
 */
public class UserIDHeader extends BrokerTestCase {
    private static final AMQP.BasicProperties GOOD = new AMQP.BasicProperties.Builder().
            userId("guest").build();
    private static final AMQP.BasicProperties BAD = new AMQP.BasicProperties.Builder().
            userId("not the guest, honest").build();

    ////@Test
    public void validUserId() throws IOException {
        publish(GOOD);
    }

    ////@Test
    public void invalidUserId() {
        try {
            publish(BAD);
            fail("Accepted publish with incorrect user ID");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        } catch (AlreadyClosedException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    ////@Test
    public void impersonatedUserId() throws IOException, TimeoutException {
        Host.rabbitmqctl("set_user_tags guest administrator impersonator");
        try (Connection c = connectionFactory.newConnection()) {
            publish(BAD, c.createChannel());
        } finally {
            Host.rabbitmqctl("set_user_tags guest administrator");
        }
    }

    private void publish(AMQP.BasicProperties properties) throws IOException {
        publish(properties, this.channel);
    }

    private void publish(AMQP.BasicProperties properties, Channel channel) throws IOException {
        channel.basicPublish("amq.fanout", "", properties, "".getBytes());
        channel.queueDeclare(); // To flush the channel
    }
}
