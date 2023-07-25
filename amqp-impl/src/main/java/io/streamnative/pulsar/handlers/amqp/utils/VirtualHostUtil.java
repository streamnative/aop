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
package io.streamnative.pulsar.handlers.amqp.utils;

import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import java.util.StringTokenizer;
import org.apache.commons.lang3.tuple.Pair;

/**
 * VirtualHostUtil
 */
public class VirtualHostUtil {

    private static final String L = "/";

    public static Pair<String, String> getTenantAndNamespace(String virtualHostStr, String tenant) {
        String virtualHost = virtualHostStr.trim();
        if (L.equals(virtualHost)) {
            return Pair.of(tenant, AmqpConnection.DEFAULT_NAMESPACE);
        }
        StringTokenizer tokenizer = new StringTokenizer(virtualHost, L, false);
        return switch (tokenizer.countTokens()) {
            case 1 -> Pair.of(tenant, tokenizer.nextToken());
            case 2 -> Pair.of(tokenizer.nextToken(), tokenizer.nextToken());
            default -> null;
        };
    }
}
