
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

import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;

public class Nowait extends BrokerTestCase {
    public void testQueueDeclareWithNowait() throws IOException {
        String q = generateQueueName();
        channel.queueDeclareNoWait(q, false, true, true, null);
        channel.queueDeclarePassive(q);
    }

    public void testQueueBindWithNowait() throws IOException {
        String q = generateQueueName();
        channel.queueDeclareNoWait(q, false, true, true, null);
        channel.queueBindNoWait(q, "amq.fanout", "", null);
    }

    public void testExchangeDeclareWithNowait() throws IOException {
        String x = generateExchangeName();
        try {
            channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);
            channel.exchangeDeclarePassive(x);
        } finally {
            channel.exchangeDelete(x);
        }
    }

    public void testExchangeBindWithNowait() throws IOException {
        String x = generateExchangeName();
        try {
            channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);
            channel.exchangeBindNoWait(x, "amq.fanout", "", null);
        } finally {
            channel.exchangeDelete(x);
        }
    }

    public void testExchangeUnbindWithNowait() throws IOException {
        String x = generateExchangeName();
        try {
            channel.exchangeDeclare(x, "fanout", false, false, false, null);
            channel.exchangeBind(x, "amq.fanout", "", null);
            channel.exchangeUnbindNoWait(x, "amq.fanout", "", null);
        } finally {
            channel.exchangeDelete(x);
        }
    }

    public void testQueueDeleteWithNowait() throws IOException {
        String q = generateQueueName();
        channel.queueDeclareNoWait(q, false, true, true, null);
        channel.queueDeleteNoWait(q, false, false);
    }

    public void testExchangeDeleteWithNowait() throws IOException {
        String x = generateExchangeName();
        channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);
        channel.exchangeDeleteNoWait(x, false);
    }
}
