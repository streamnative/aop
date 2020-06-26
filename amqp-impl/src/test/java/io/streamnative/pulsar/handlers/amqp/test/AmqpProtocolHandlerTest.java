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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.TimeoutException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for Aop handler.
 */
public class AmqpProtocolHandlerTest extends AopProtocolHandlerTestBase {
    private ConnectionFactory factory;

    public AmqpProtocolHandlerTest() throws MalformedURLException {
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("root");
        factory.setPassword("root123");
        factory.setVirtualHost("vhost1");
    }

    @AfterMethod
    @Override protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 1000 * 5)
    public void testConnection() throws IOException, TimeoutException {
        Connection connection = factory.newConnection();
        Assert.assertTrue(connection != null);
        connection.close();
    }

    @Test(timeOut = 1000 * 5)
    public void testChannel() throws IOException, TimeoutException {
        Connection connection = factory.newConnection();
        Assert.assertTrue(connection != null);
        Channel channel = connection.createChannel();
        Assert.assertTrue(channel != null);
        channel.close();
        connection.close();

    }
}
