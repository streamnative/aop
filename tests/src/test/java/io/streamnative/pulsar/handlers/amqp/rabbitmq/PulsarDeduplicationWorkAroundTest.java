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
package io.streamnative.pulsar.handlers.amqp.rabbitmq;

import io.streamnative.pulsar.handlers.amqp.AmqpTestBase;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * RabbitMQ messaging test.
 */
@Slf4j
public class PulsarDeduplicationWorkAroundTest extends AmqpTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setBrokerDeduplicationEnabled(true);
        super.setup();
    }

    @Test(timeOut = 1000 * 5)
    public void basicConsumeCase() throws Exception {
        basicDirectConsume("vhost1", false);
    }

}
