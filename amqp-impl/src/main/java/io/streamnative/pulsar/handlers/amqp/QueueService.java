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

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;

public interface QueueService {

    void queueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
                      boolean autoDelete, boolean nowait, FieldTable arguments);

    void queueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait);

    void queueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                   boolean nowait, FieldTable argumentsTable);

    void queueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
                     FieldTable arguments);

    void queuePurge(AMQShortString queue, boolean nowait);
}
