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
        private Map<String, Object> arguments;
        private boolean auto_delete;
        private BackingQueueStatusBean backing_queue_status;
        private Object consumer_utilisation;
        private int consumers;
        private boolean durable;
        private EffectivePolicyDefinitionBean effective_policy_definition;
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
        private MessagesDetailsBean messages_details;
        private int messages_paged_out;
        private int messages_persistent;
        private int messages_ram;
        private long messages_ready;
        private MessagesReadyDetailsBean messages_ready_details;
        private int messages_ready_ram;
        private long messages_unacknowledged;
        private MessagesUnacknowledgedDetailsBean messages_unacknowledged_details;
        private int messages_unacknowledged_ram;
        private String name;
        private String node;
        private Object operator_policy;
        private Object policy;
        private Object recoverable_slaves;
        private long reductions;
        private ReductionsDetailsBean reductions_details;
        private String state;
        private String vhost;
        private MessageStatsBean message_stats;

        @NoArgsConstructor
        @Data
        public static class ArgumentsBean {
        }

        @NoArgsConstructor
        @Data
        public static class BackingQueueStatusBean {
        }

        @NoArgsConstructor
        @Data
        public static class EffectivePolicyDefinitionBean {
        }

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
        public static class MessagesDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class MessagesReadyDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class MessagesUnacknowledgedDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class ReductionsDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class MessageStatsBean {
            private int ack;
            private AckDetailsBean ack_details;
            private int deliver;
            private DeliverDetailsBean deliver_details;
            private int deliver_get;
            private DeliverGetDetailsBean deliver_get_details;
            private int deliver_no_ack;
            private DeliverNoAckDetailsBean deliver_no_ack_details;
            private int get;
            private GetDetailsBean get_details;
            private int get_no_ack;
            private GetNoAckDetailsBean get_no_ack_details;
            private int redeliver;
            private RedeliverDetailsBean redeliver_details;
            private long publish;
            private PublishDetailsBean publish_details;

            @NoArgsConstructor
            @Data
            public static class PublishDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class AckDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class DeliverDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class DeliverGetDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class DeliverNoAckDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class GetDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class GetNoAckDetailsBean {
                private double rate;
            }

            @NoArgsConstructor
            @Data
            public static class RedeliverDetailsBean {
                private double rate;
            }
        }
    }
}
