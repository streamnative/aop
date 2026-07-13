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

import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Protocol advertise data format tests.
 */
public class AmqpProtocolDataTest {

    @Test
    public void testAdvertiseDataIncludesAdminPort() {
        AmqpServiceConfiguration conf = new AmqpServiceConfiguration();
        conf.setAmqpListeners("amqp://127.0.0.1:5672");
        conf.setAmqpAdminPort(15673);

        AmqpProtocolHandler handler = new AmqpProtocolHandler();
        try {
            handler.initialize(conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        String protocolData = handler.getProtocolDataToAdvertise();
        Assert.assertEquals(protocolData, "amqp://127.0.0.1:5672|15673");
        Assert.assertEquals(AmqpProtocolHandler.extractAmqpListeners(protocolData), "amqp://127.0.0.1:5672");
        Assert.assertEquals(AmqpProtocolHandler.extractAmqpAdminPort(protocolData).orElse(-1).intValue(), 15673);
    }

    @Test
    public void testLegacyProtocolDataCompatible() {
        String legacy = "amqp://127.0.0.1:5672";
        Assert.assertEquals(AmqpProtocolHandler.extractAmqpListeners(legacy), legacy);
        Assert.assertFalse(AmqpProtocolHandler.extractAmqpAdminPort(legacy).isPresent());
    }
}
