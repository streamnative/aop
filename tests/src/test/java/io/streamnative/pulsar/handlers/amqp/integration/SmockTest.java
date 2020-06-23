package io.streamnative.pulsar.handlers.amqp.integration;

import io.streamnative.pulsar.handlers.amqp.integration.topologies.PulsarClusterTestBase;
import org.testng.ITest;

public class SmockTest extends PulsarClusterTestBase implements ITest {



    @Override
    public String getTestName() {
        return "Smock test";
    }
}
