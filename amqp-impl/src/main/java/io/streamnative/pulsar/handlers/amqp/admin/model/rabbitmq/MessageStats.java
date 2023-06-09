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
package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MessageStats {

    private long ack;
    private String ack_details;
    private long confirm;
    private String confirm_details;
    private long deliver;
    private String deliver_details;
    private long deliver_get;
    private long deliver_get_details;
    private long deliver_no_ack;
    private long deliver_no_ack_details;
    private long get;
    private long get_details;
    private long get_no_ack;
    private long get_no_ack_details;
    private long publish;
    private long publish_details;
    private long redeliver;
    private long redeliver_details;
    private long return_unroutable;
    private long return_unroutable_details;
}
