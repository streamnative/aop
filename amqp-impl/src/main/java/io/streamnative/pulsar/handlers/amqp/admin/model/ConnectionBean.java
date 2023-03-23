package io.streamnative.pulsar.handlers.amqp.admin.model;

import java.util.List;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@Data
public class ConnectionBean {

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
        private String auth_mechanism;
        private long channel_max;
        private long channels;
        private ClientPropertiesBean client_properties;
        private long connected_at;
        private int frame_max;
        private GarbageCollectionBean garbage_collection;
        private String host;
        private String name;
        private String node;
        private Object peer_cert_issuer;
        private Object peer_cert_subject;
        private Object peer_cert_validity;
        private String peer_host;
        private int peer_port;
        private int port;
        private String protocol;
        private int recv_cnt;
        private int recv_oct;
        private RecvOctDetailsBean recv_oct_details;
        private int reductions;
        private ReductionsDetailsBean reductions_details;
        private int send_cnt;
        private int send_oct;
        private SendOctDetailsBean send_oct_details;
        private int send_pend;
        private boolean ssl;
        private Object ssl_cipher;
        private Object ssl_hash;
        private Object ssl_key_exchange;
        private Object ssl_protocol;
        private String state;
        private int timeout;
        private String type;
        private String user;
        private String user_provided_name;
        private String user_who_performed_action;
        private String vhost;

        @NoArgsConstructor
        @Data
        public static class ClientPropertiesBean {
            private CapabilitiesBean capabilities;
            private String connection_name;
            private String copyright;
            private String information;
            private String platform;
            private String product;
            private String version;

            @NoArgsConstructor
            @Data
            public static class CapabilitiesBean {
                private boolean authentication_failure_close;
                private boolean _$BasicNack233; // FIXME check this code
                private boolean _$ConnectionBlocked258; // FIXME check this code
                private boolean consumer_cancel_notify;
                private boolean exchange_exchange_bindings;
                private boolean publisher_confirms;
            }
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
        public static class RecvOctDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class ReductionsDetailsBean {
            private double rate;
        }

        @NoArgsConstructor
        @Data
        public static class SendOctDetailsBean {
            private double rate;
        }
    }
}
