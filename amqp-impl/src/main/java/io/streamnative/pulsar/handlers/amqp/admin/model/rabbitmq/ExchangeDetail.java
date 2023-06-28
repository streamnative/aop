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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ExchangeDetail {

    private Map<String, Object> arguments;
    private boolean auto_delete;
    private boolean durable;
    private boolean internal;
    private MessageStatsBean message_stats;
    private String name;
    private String fullName;
    private String type;
    private String user_who_performed_action;
    private String vhost;
    private List<?> incoming;
    private List<?> outgoing;

    @NoArgsConstructor
    @Data
    public static class MessageStatsBean {
        private long publish_in;
        private PublishInDetailsBean publish_in_details;
        private long publish_out;
        private PublishOutDetailsBean publish_out_details;

        @NoArgsConstructor
        @Data
        public static class PublishInDetailsBean {
            private double avg;
            private double avg_rate;
            private double rate;
            private List<SamplesBean> samples = Lists.newArrayList();
        }

        @NoArgsConstructor
        @Data
        public static class PublishOutDetailsBean {
            private double avg;
            private double avg_rate;
            private double rate;
            private List<SamplesBean> samples = Lists.newArrayList();

        }
    }
}
