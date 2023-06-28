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
public class QueuesList {


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
        private String fullName;
        private Map<String, Object> arguments;
        private boolean auto_delete;
        private Map<String, Object> backing_queue_status;
        private Object consumer_utilisation;
        private int consumers;
        private boolean durable;
        private Map<String, Object> effective_policy_definition;
        private boolean exclusive;
        private Object exclusive_consumer_tag;
        private GarbageCollectionBean garbage_collection;
        private Object head_message_timestamp;
        private String idle_since;
        private int memory;
        private int message_bytes;
        private int message_bytes_paged_out;
        private int message_bytes_persistent;
        private int message_bytes_ram;
        private int message_bytes_ready;
        private int message_bytes_unacknowledged;
        private long messages;
        private RateBean messages_details;
        private int messages_paged_out;
        private int messages_persistent;
        private int messages_ram;
        private long messages_ready;
        private RateBean messages_ready_details;
        private int messages_ready_ram;
        private long messages_unacknowledged;
        private RateBean messages_unacknowledged_details;
        private int messages_unacknowledged_ram;
        private String name;
        private String node;
        private Object operator_policy;
        private Object policy;
        private Object recoverable_slaves;
        private long reductions;
        private RateBean reductions_details;
        private String state;
        private String vhost;
        private MessageStatsBean message_stats;

        @NoArgsConstructor
        @Data
        public static class GarbageCollectionBean {
            private int fullsweep_after;
            private int max_heap_size;
            private int min_bin_vheap_size;
            private int min_heap_size;
            private int minor_gcs;
        }

        @NoArgsConstructor
        @Data
        public static class MessageStatsBean {
            private int ack;
            private RateBean ack_details;
            private int deliver;
            private RateBean deliver_details;
            private int deliver_get;
            private RateBean deliver_get_details;
            private int deliver_no_ack;
            private RateBean deliver_no_ack_details;
            private int get;
            private RateBean get_details;
            private int get_no_ack;
            private RateBean get_no_ack_details;
            private int redeliver;
            private RateBean redeliver_details;
            private long publish;
            private RateBean publish_details;
        }
    }
    @NoArgsConstructor
    @Data
    public static class RateBean {
        private double rate;
    }
}
