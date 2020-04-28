
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

import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
import java.util.HashMap;
import org.junit.Test;

public class HeadersExchangeValidation extends BrokerTestCase {

    @Test
    public void headersValidation() throws IOException {
        AMQP.Queue.DeclareOk ok = channel.queueDeclare();
        String queue = ok.getQueue();

        HashMap<String, Object> arguments = new HashMap<String, Object>();
        succeedBind(queue, arguments);

        arguments.put("x-match", 23);
        failBind(queue, arguments);

        arguments.put("x-match", "all or any I don't mind");
        failBind(queue, arguments);

        arguments.put("x-match", "all");
        succeedBind(queue, arguments);

        arguments.put("x-match", "any");
        succeedBind(queue, arguments);
    }

    private void failBind(String queue, HashMap<String, Object> arguments) {
        try {
            Channel ch = connection.createChannel();
            ch.queueBind(queue, "amq.headers", "", arguments);
            fail("Expected failure");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.PRECONDITION_FAILED, e);
        }
    }

    private void succeedBind(String queue, HashMap<String, Object> arguments) throws IOException {
        Channel ch = connection.createChannel();
        ch.queueBind(queue, "amq.headers", "", arguments);
        ch.abort();
    }
}
