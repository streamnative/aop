
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

import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import org.junit.Test;

public class DoubleDeletion extends BrokerTestCase {
    protected static final String Q = "DoubleDeletionQueue";
    protected static final String X = "DoubleDeletionExchange";

    @Test
    public void doubleDeletionQueue()
            throws IOException {
        channel.queueDelete(Q);
        channel.queueDeclare(Q, false, false, false, null);
        channel.queueDelete(Q);
        channel.queueDelete(Q);
    }

    @Test
    public void doubleDeletionExchange()
            throws IOException {
        channel.exchangeDelete(X);
        channel.exchangeDeclare(X, "direct");
        channel.exchangeDelete(X);
        channel.exchangeDelete(X);
    }
}
