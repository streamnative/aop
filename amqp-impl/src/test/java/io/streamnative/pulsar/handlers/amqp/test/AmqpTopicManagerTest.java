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

package io.streamnative.pulsar.handlers.amqp.test;

import static org.testng.Assert.assertFalse;

import io.streamnative.pulsar.handlers.amqp.AmqpTopicManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.AbstractTopic;
import org.testng.annotations.Test;

/**
 * Unit test for Pulsar Topic.
 */
public class AmqpTopicManagerTest extends AmqpProtocolTestBase {

    @Test
    public void testDeleteWhileInactiveIsFalse() {
        PulsarService pulsarService = connection.getPulsarService();
        AmqpTopicManager amqpTopicManager = new AmqpTopicManager(pulsarService);
        AbstractTopic abstractTopic = (AbstractTopic) amqpTopicManager.getOrCreateTopic(
                "public/vhost1/test", true);
        assertFalse(abstractTopic.isDeleteWhileInactive());
    }
}
