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

import io.streamnative.pulsar.handlers.amqp.impl.HeadersMessageRouter;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *  unit test for headers ManageRouter.
 */
public class AmqpHeadersMessageRouterTest {
    @Test
    public void testAllMatchWithNoEmptyValue(){
        HeadersMessageRouter headersMessageRouter = new HeadersMessageRouter(null);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("format", "pdf");
        arguments.put("type", "report");
        arguments.put("x-match", "all");
        headersMessageRouter.setArguments(arguments);

        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "pdf");
        headers.put("type", "report");
        Assert.assertTrue(headersMessageRouter.isMatch(headers));

        Map<String, Object> headers2 = new HashMap<>();
        headers2.put("format", "pdf");
        headers2.put("type", "error");
        Assert.assertFalse(headersMessageRouter.isMatch(headers2));
    }

    @Test
    public void testAllMatchWithEmptyValue(){
        HeadersMessageRouter headersMessageRouter = new HeadersMessageRouter(null);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("format", "");
        arguments.put("type", "report");
        arguments.put("x-match", "all");
        headersMessageRouter.setArguments(arguments);

        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "pdf");
        headers.put("type", "report");
        Assert.assertTrue(headersMessageRouter.isMatch(headers));

        Map<String, Object> headers2 = new HashMap<>();
        headers2.put("format", "pdf");
        headers2.put("type", "error");
        Assert.assertFalse(headersMessageRouter.isMatch(headers2));
    }

    @Test
    public void testAnyMatchWithNoEmptyValue(){
        HeadersMessageRouter headersMessageRouter = new HeadersMessageRouter(null);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("format", "pdf");
        arguments.put("type", "report");
        arguments.put("x-match", "any");
        headersMessageRouter.setArguments(arguments);

        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "pdf");
        Assert.assertTrue(headersMessageRouter.isMatch(headers));

        Map<String, Object> headers2 = new HashMap<>();
        headers2.put("format", "pdf111");
        headers2.put("type", "error");
        Assert.assertFalse(headersMessageRouter.isMatch(headers2));
    }

    @Test
    public void testAnyMatchWithEmptyValue(){
        HeadersMessageRouter headersMessageRouter = new HeadersMessageRouter(null);
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("format", "");
        arguments.put("type", "report");
        arguments.put("x-match", "any");
        headersMessageRouter.setArguments(arguments);

        Map<String, Object> headers = new HashMap<>();
        headers.put("format", "pdf");
        Assert.assertTrue(headersMessageRouter.isMatch(headers));

        Map<String, Object> headers2 = new HashMap<>();
        headers2.put("type", "error");
        Assert.assertFalse(headersMessageRouter.isMatch(headers2));
    }
}
