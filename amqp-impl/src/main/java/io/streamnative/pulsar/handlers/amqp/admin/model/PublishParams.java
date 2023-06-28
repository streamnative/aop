/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

@Data
@NoArgsConstructor
public class PublishParams {

    @JsonProperty("delivery_mode")
    private String deliveryMode;
    @JsonProperty("routing_key")
    private String routingKey;
    @JsonProperty("payload_encoding")
    private String payloadEncoding;
    private String name;
    private String payload;
    private String vhost;
    private Map<String, Object> headers;
    private Map<String, Object> properties;
    private Map<String, String> props;
}
