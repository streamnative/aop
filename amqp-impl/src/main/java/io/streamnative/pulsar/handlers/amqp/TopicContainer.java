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
package io.streamnative.pulsar.handlers.amqp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.zookeeper.ZooKeeper;

public class TopicContainer {

    public static PulsarService pulsarService;
    public static AmqpTopicManager amqpTopicManager;
    public static ZooKeeper zooKeeper;
    public final static ObjectMapper jsonMapper = ObjectMapperFactory.create();

    public static void init(PulsarService pulsarService) {
        TopicContainer.pulsarService = pulsarService;
        TopicContainer.amqpTopicManager = new AmqpTopicManager(pulsarService);
        TopicContainer.zooKeeper = pulsarService.getLocalZkCache().getZooKeeper();
    }

}
