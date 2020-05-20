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
