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

import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * AMQP proxy related test.
 */
@Slf4j
public class ProxyTest extends RabbitMQTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        setBrokerCount(3);
        super.setup();
    }

    //@Test
    public void proxyBasicTest() throws Exception {

        basicFanoutTest("test1", "vhost1", false, 2);
        basicFanoutTest("test2", "vhost2", false, 3);
        basicFanoutTest("test3", "vhost3", false, 2);

        CountDownLatch countDownLatch = new CountDownLatch(3);
        new Thread(() -> {
            try {
                basicFanoutTest("test4", "vhost1", false, 2);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test4 error for vhost1.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                basicFanoutTest("test5", "vhost2", false, 3);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test5 error for vhost2.", e);
            }
        }).start();
        new Thread(() -> {
            try {
                basicFanoutTest("test6", "vhost3", false, 3);
                countDownLatch.countDown();
            } catch (Exception e) {
                log.error("Test6 error for vhost3.", e);
            }
        }).start();
        countDownLatch.await();
    }

    @Test
    public void unloadBundleTest() throws Exception {
        basicFanoutTest("test1", "vhost1", true, 3);
        basicFanoutTest("test2", "vhost2", true, 2);
        basicFanoutTest("test3", "vhost3", true, 2);
    }

}
