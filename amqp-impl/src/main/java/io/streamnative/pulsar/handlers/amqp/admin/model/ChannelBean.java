package io.streamnative.pulsar.handlers.amqp.admin.model;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ChannelBean {
    private int total_count;
    private int item_count;
    private int filtered_count;
    private int page;
    private int page_size;
    private int page_count;
    private List<ItemsBean> items;

    @NoArgsConstructor
    @Data
    public static class ItemsBean {
        private int acks_uncommitted;
        private boolean confirm;
        private ConnectionDetailsBean connection_details;
        private int consumer_count;
        private GarbageCollectionBean garbage_collection;
        private int global_prefetch_count;
        private String idle_since;
        private int messages_unacknowledged;
        private int messages_uncommitted;
        private int messages_unconfirmed;
        private String name;
        private String node;
        private int number;
        private long prefetch_count;
        private int reductions;
        private ReductionsDetailsBean reductions_details;
        private String state;
        private boolean transactional;
        private String user;
        private String user_who_performed_action;
        private String vhost;

        @NoArgsConstructor
        @Data
        public static class ConnectionDetailsBean {
            private String name;
            private String peer_host;
            private int peer_port;
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
        public static class ReductionsDetailsBean {
            private double rate;
        }
    }
}
