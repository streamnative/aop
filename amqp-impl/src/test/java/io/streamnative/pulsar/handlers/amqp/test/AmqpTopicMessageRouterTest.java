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

import io.streamnative.pulsar.handlers.amqp.impl.TopicMessageRouter;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *  unit test for topicManageRouter.
 */
public class AmqpTopicMessageRouterTest {

    @Test
    public void isMatchTest() {
        String bindingKey1 = "*.b.*";
        String bindingKey2 = "a.*.*";
        String bindingKey3 = "#.c";
        TopicMessageRouter router = new TopicMessageRouter(null);
        router.addBindingKey(bindingKey1);
        router.addBindingKey(bindingKey2);
        router.addBindingKey(bindingKey3);
        Assert.assertTrue(router.isMatch("a.b.c"));
        Assert.assertTrue(router.isMatch("test1.b.test2"));
        Assert.assertTrue(router.isMatch("a.test1.test2"));
        Assert.assertTrue(router.isMatch("test1.test2.a.b.c"));
        Assert.assertFalse(router.isMatch("a.d.test1.test2"));
    }

}
