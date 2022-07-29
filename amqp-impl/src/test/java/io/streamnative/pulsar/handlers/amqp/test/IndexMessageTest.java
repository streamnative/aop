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

import io.streamnative.pulsar.handlers.amqp.IndexMessage;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit tests for index message.
 */
public class IndexMessageTest {

    @Test
    public void testEncodeAndDecode() {
        IndexMessage indexMessage = IndexMessage.create("test", 1L, 1L, null);
        byte[] bytes = indexMessage.encode();
        Assert.assertNotNull(bytes);
        IndexMessage decoded = IndexMessage.create(bytes);
        Assert.assertNotNull(decoded);
        Assert.assertEquals(decoded, indexMessage);
        indexMessage.recycle();
        decoded.recycle();
    }
}
