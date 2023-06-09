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
import lombok.Data;
import lombok.NoArgsConstructor;


@NoArgsConstructor
@Data
public class OverviewBean {

    private String management_version;
    private String rates_mode;
    private SampleRetentionPoliciesBean sample_retention_policies;
    private String rabbitmq_version;
    private String cluster_name;
    private String erlang_version;
    private String erlang_full_version;
    private MessageStatsBean message_stats;
    private ChurnRatesBean churn_rates;
    private QueueTotalsBean queue_totals;
    private ObjectTotalsBean object_totals;
    private int statistics_db_event_queue;
    private String node;
    private List<ExchangeTypesBean> exchange_types;
    private List<ListenersBean> listeners;
    private List<ContextsBean> contexts;

    @NoArgsConstructor
    @Data
    public static class SampleRetentionPoliciesBean {
        private List<Integer> global;
        private List<Integer> basic;
        private List<Integer> detailed;
    }

    @NoArgsConstructor
    @Data
    public static class RateBean {
        private double rate;
        private double avg_rate;
        private double avg;
        private List<SamplesBean> samples = Lists.newArrayList();
    }

    @NoArgsConstructor
    @Data
    public static class MessageStatsBean {
        private long ack;
        private RateBean ack_details;
        private long confirm;
        private RateBean confirm_details;
        private long deliver;
        private RateBean deliver_details;
        private long deliver_get;
        private RateBean deliver_get_details;
        private int deliver_no_ack;
        private RateBean deliver_no_ack_details;
        private int disk_reads;
        private RateBean disk_reads_details;
        private long disk_writes;
        private RateBean disk_writes_details;
        private int get;
        private RateBean get_details;
        private int get_no_ack;
        private RateBean get_no_ack_details;
        private long publish;
        private RateBean publish_details;
        private long redeliver;
        private RateBean redeliver_details;
        private int return_unroutable;
        private RateBean return_unroutable_details;
    }

    @NoArgsConstructor
    @Data
    public static class ChurnRatesBean {
        private int channel_closed;
        private RateBean channel_closed_details;
        private int channel_created;
        private RateBean channel_created_details;
        private int connection_closed;
        private RateBean connection_closed_details;
        private int connection_created;
        private RateBean connection_created_details;
        private int queue_created;
        private RateBean queue_created_details;
        private int queue_declared;
        private RateBean queue_declared_details;
        private int queue_deleted;
        private RateBean queue_deleted_details;

    }

    @NoArgsConstructor
    @Data
    public static class QueueTotalsBean {
        private long messages;
        private RateBean messages_details;
        private long messages_ready;
        private RateBean messages_ready_details;
        private long messages_unacknowledged;
        private RateBean messages_unacknowledged_details;

    }

    @NoArgsConstructor
    @Data
    public static class ObjectTotalsBean {
        private int channels;
        private int connections;
        private int consumers;
        private int exchanges;
        private int queues;
    }

    @NoArgsConstructor
    @Data
    public static class ExchangeTypesBean {
        private String name;
        private String description;
        private boolean enabled;
    }

    @NoArgsConstructor
    @Data
    public static class ListenersBean {
        private String node;
        private String protocol;
        private String ip_address;
        private int port;
        private SocketOptsBean socket_opts;

        @NoArgsConstructor
        @Data
        public static class SocketOptsBean {
            private int backlog;
            private boolean nodelay;
            private boolean exit_on_close;
            private List<Boolean> linger;
        }
    }

    @NoArgsConstructor
    @Data
    public static class ContextsBean {
        private SslOptsBean ssl_opts;
        private String node;
        private String description;
        private String path;
        private String cowboy_opts;
        private String ip;
        private String port;

        @NoArgsConstructor
        @Data
        public static class SslOptsBean {
            private String keyfile;
            private String certfile;
            private String cacertfile;
        }
    }
}
