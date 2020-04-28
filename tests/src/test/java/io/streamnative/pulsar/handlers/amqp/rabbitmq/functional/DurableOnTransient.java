
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

import static org.junit.Assert.assertNotNull;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import org.junit.Test;

public class DurableOnTransient extends ClusteredTestBase {
    protected static final String Q = "SemiDurableBindings.DurableQueue";
    protected static final String X = "SemiDurableBindings.TransientExchange";

    private GetResponse basicGet()
            throws IOException {
        return channel.basicGet(Q, true);
    }

    private void basicPublish()
            throws IOException {
        channel.basicPublish(X, "",
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                "persistent message".getBytes());
    }

    protected void createResources() throws IOException {
        channel.exchangeDelete(X);
        // transient exchange
        channel.exchangeDeclare(X, "direct", false);

        channel.queueDelete(Q);
        // durable queue
        channel.queueDeclare(Q, true, false, false, null);
    }

    protected void releaseResources() throws IOException {
        channel.queueDelete(Q);
        channel.exchangeDelete(X);
    }

    @Test
    public void bindDurableToTransient()
            throws IOException {
        channel.queueBind(Q, X, "");
        basicPublish();
        assertNotNull(basicGet());
    }

    @Test
    public void semiDurableBindingRemoval() throws IOException {
        if (clusteredConnection != null) {
            deleteExchange("x");
            declareTransientTopicExchange("x");
            clusteredChannel.queueDelete("q");
            clusteredChannel.queueDeclare("q", true, false, false, null);
            channel.queueBind("q", "x", "k");

            stopSecondary();

            deleteExchange("x");

            startSecondary();

            declareTransientTopicExchange("x");

            basicPublishVolatile("x", "k");
            assertDelivered("q", 0);

            deleteQueue("q");
            deleteExchange("x");
        }
    }
}
