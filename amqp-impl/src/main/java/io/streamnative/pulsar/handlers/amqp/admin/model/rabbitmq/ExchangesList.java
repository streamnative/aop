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

import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ExchangesList {

    private int filtered_count;
    private int item_count;
    private int page;
    private int page_count;
    private int page_size;
    private int total_count;
    private List<ItemsBean> items;

    @NoArgsConstructor
    @Data
    public static class ItemsBean {
        private Map<String, Object> arguments;
        private boolean auto_delete;
        private boolean durable;
        private boolean internal;
        private String name;
        private String fullName;
        private String type;
        private String user_who_performed_action;
        private String vhost;
        private MessageStatsBean message_stats;

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
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class PublishOutDetailsBean {
                private double rate;
            }
        }
    }
}
