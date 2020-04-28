

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

package io.streamnative.pulsar.handlers.amqp.functional;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;

public class ExchangeDeletePredeclared extends BrokerTestCase {
    public void testDeletingPredeclaredAmqExchange() throws IOException {
        try {
            channel.exchangeDelete("amq.fanout");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, e);
        }
    }

    public void testDeletingPredeclaredAmqRabbitMQExchange() throws IOException {
        try {
            channel.exchangeDelete("amq.rabbitmq.log");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.ACCESS_REFUSED, e);
        }
    }
}
