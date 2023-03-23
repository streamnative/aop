package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

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
            private List<SamplesBean> samples;

            @NoArgsConstructor
            @Data
            public static class SamplesBean {
                private int sample;
                private long timestamp;
            }
        }

        @NoArgsConstructor
        @Data
        public static class PublishOutDetailsBean {
            private double avg;
            private double avg_rate;
            private double rate;
            private List<SamplesBeanX> samples;

            @NoArgsConstructor
            @Data
            public static class SamplesBeanX {
                private int sample;
                private long timestamp;
            }
        }
    }
}
