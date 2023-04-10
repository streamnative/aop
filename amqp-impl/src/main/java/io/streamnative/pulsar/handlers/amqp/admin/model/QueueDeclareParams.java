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
package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This class is used to declare queue params.
 */
@Data
@NoArgsConstructor
public class QueueDeclareParams {

    @JsonProperty("auto_delete")
    private boolean autoDelete;
    private boolean durable;
    private boolean exclusive;
    private boolean passive;
    private String node;
    private Map<String, Object> arguments;
    private String vhost;
    private String name;
}
