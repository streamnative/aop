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
package io.streamnative.pulsar.handlers.amqp.impl;

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Direct message router.
 */
public class DirectMessageRouter extends AbstractAmqpMessageRouter {

    public DirectMessageRouter() {
        super(Type.Direct);
    }

    @Override
    public CompletableFuture<Void> routingMessage(long ledgerId, long entryId, String routingKey,
                                                  Map<String, Object> properties) {
        if (this.bindingKeys.contains(routingKey)) {
            return queue.writeIndexMessageAsync(exchange.getName(), ledgerId, entryId);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }
}
