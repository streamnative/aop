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

import io.streamnative.pulsar.handlers.amqp.utils.ExchangeType;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Properties for queue.
 */
@Data
@NoArgsConstructor
public class AmqpExchangeProperties {
    private String exchangeName;
    private ExchangeType type;
    private Map<String, AmqpBinding> bindings;
    private Map<String, Object> arguments;
}
