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
 * This class is used to as return value of the queue list admin api.
 */
@Data
@NoArgsConstructor
public class QueueBean {

    private String name;
    private String vhost;
    private boolean durable;
    private boolean exclusive;
    @JsonProperty("auto_delete")
    private boolean autoDelete;
    private Map<String, Object> arguments;

}
