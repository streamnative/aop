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
        private String type;
        private String user_who_performed_action;
        private String vhost;
        private MessageStatsBean message_stats;

        @NoArgsConstructor
        @Data
        public static class ArgumentsBean {
        }

        @NoArgsConstructor
        @Data
        public static class MessageStatsBean {
            private int publish_in;
            private PublishInDetailsBean publish_in_details;
            private int publish_out;
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
