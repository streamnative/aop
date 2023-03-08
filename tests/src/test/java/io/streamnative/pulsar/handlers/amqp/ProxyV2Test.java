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

import io.streamnative.pulsar.handlers.amqp.rabbitmq.RabbitMQTestCase;
import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * AMQP proxy related test.
 */
@Slf4j
public class ProxyV2Test extends AmqpTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        setBrokerCount(3);
        ((AmqpServiceConfiguration) this.conf).setAmqpProxyV2Enable(true);
        super.setup();
    }

    @Test
    public void rabbitMQProxyTest() throws Exception {
        int proxyPort = getProxyPort();
        RabbitMQTestCase rabbitMQTestCase = new RabbitMQTestCase(admin);

        rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test1",
                "vhost1", false, 2);
        rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test2",
                "vhost2", false, 3);
        rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test3",
                "vhost3", false, 2);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        new Thread(() -> {
            try {
                rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test4",
                        "vhost1", false, 2);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test4 error for vhost1.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test5",
                        "vhost2", false, 3);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test5 error for vhost2.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                rabbitMQTestCase.basicFanoutTest(proxyPort, "rabbitmq-proxy-test6",
                        "vhost3", false, 3);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test6 error for vhost3.", e);
            }
        }).start();
        countDownLatch.await();
    }

    @Test
    public void unloadBundleTest() throws Exception {
        int proxyPort = getProxyPort();

        RabbitMQTestCase rabbitMQTestCase = new RabbitMQTestCase(admin);
        rabbitMQTestCase.basicFanoutTest(proxyPort, "unload-bundle-test1",
                "vhost1", true, 3);
        rabbitMQTestCase.basicFanoutTest(proxyPort, "unload-bundle-test2",
                "vhost2", true, 2);
        rabbitMQTestCase.basicFanoutTest(proxyPort, "unload-bundle-test3",
                "vhost3", true, 2);
    }

}
