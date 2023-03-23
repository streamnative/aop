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
package io.streamnative.pulsar.handlers.amqp.admin.model;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This class is used to as return value of the vhost list admin api.
 */
@Data
@NoArgsConstructor
@SuppressWarnings("all")
public class VhostBean {

    private ClusterStateBean cluster_state;
    private String name;
    private boolean tracing;
    private MessageStatsBean message_stats;
    private int messages;
    private MessagesDetailsBean messages_details;
    private int messages_ready;
    private MessagesReadyDetailsBean messages_ready_details;
    private int messages_unacknowledged;
    private MessagesUnacknowledgedDetailsBean messages_unacknowledged_details;
    private long recv_oct;
    private RecvOctDetailsBean recv_oct_details;
    private long send_oct;
    private SendOctDetailsBean send_oct_details;

    public ClusterStateBean getCluster_state() {
        return cluster_state;
    }

    public void setCluster_state(ClusterStateBean cluster_state) {
        this.cluster_state = cluster_state;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isTracing() {
        return tracing;
    }

    public void setTracing(boolean tracing) {
        this.tracing = tracing;
    }

    public MessageStatsBean getMessage_stats() {
        return message_stats;
    }

    public void setMessage_stats(MessageStatsBean message_stats) {
        this.message_stats = message_stats;
    }

    public int getMessages() {
        return messages;
    }

    public void setMessages(int messages) {
        this.messages = messages;
    }

    public MessagesDetailsBean getMessages_details() {
        return messages_details;
    }

    public void setMessages_details(MessagesDetailsBean messages_details) {
        this.messages_details = messages_details;
    }

    public int getMessages_ready() {
        return messages_ready;
    }

    public void setMessages_ready(int messages_ready) {
        this.messages_ready = messages_ready;
    }

    public MessagesReadyDetailsBean getMessages_ready_details() {
        return messages_ready_details;
    }

    public void setMessages_ready_details(MessagesReadyDetailsBean messages_ready_details) {
        this.messages_ready_details = messages_ready_details;
    }

    public int getMessages_unacknowledged() {
        return messages_unacknowledged;
    }

    public void setMessages_unacknowledged(int messages_unacknowledged) {
        this.messages_unacknowledged = messages_unacknowledged;
    }

    public MessagesUnacknowledgedDetailsBean getMessages_unacknowledged_details() {
        return messages_unacknowledged_details;
    }

    public void setMessages_unacknowledged_details(MessagesUnacknowledgedDetailsBean messages_unacknowledged_details) {
        this.messages_unacknowledged_details = messages_unacknowledged_details;
    }

    public long getRecv_oct() {
        return recv_oct;
    }

    public void setRecv_oct(long recv_oct) {
        this.recv_oct = recv_oct;
    }

    public RecvOctDetailsBean getRecv_oct_details() {
        return recv_oct_details;
    }

    public void setRecv_oct_details(RecvOctDetailsBean recv_oct_details) {
        this.recv_oct_details = recv_oct_details;
    }

    public long getSend_oct() {
        return send_oct;
    }

    public void setSend_oct(long send_oct) {
        this.send_oct = send_oct;
    }

    public SendOctDetailsBean getSend_oct_details() {
        return send_oct_details;
    }

    public void setSend_oct_details(SendOctDetailsBean send_oct_details) {
        this.send_oct_details = send_oct_details;
    }

    public static class ClusterStateBean {
        @SerializedName("rabbit@")
        private String _$RabbitDmsvm237b3ad3rabbitmq016; // FIXME check this code

        public String get_$RabbitDmsvm237b3ad3rabbitmq016() {
            return _$RabbitDmsvm237b3ad3rabbitmq016;
        }

        public void set_$RabbitDmsvm237b3ad3rabbitmq016(String _$RabbitDmsvm237b3ad3rabbitmq016) {
            this._$RabbitDmsvm237b3ad3rabbitmq016 = _$RabbitDmsvm237b3ad3rabbitmq016;
        }
    }

    public static class MessageStatsBean {
        /**
         * ack : 3616822
         * ack_details : {"rate":0}
         * confirm : 3616846
         * confirm_details : {"rate":0}
         * deliver : 3626717
         * deliver_details : {"rate":0}
         * deliver_get : 3627619
         * deliver_get_details : {"rate":0}
         * deliver_no_ack : 344
         * deliver_no_ack_details : {"rate":0}
         * get : 166
         * get_details : {"rate":0}
         * get_no_ack : 392
         * get_no_ack_details : {"rate":0}
         * publish : 3617391
         * publish_details : {"rate":0}
         * redeliver : 9958
         * redeliver_details : {"rate":0}
         * return_unroutable : 0
         * return_unroutable_details : {"rate":0}
         */

        private int ack;
        private AckDetailsBean ack_details;
        private int confirm;
        private ConfirmDetailsBean confirm_details;
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
        private int publish;
        private PublishDetailsBean publish_details;
        private int redeliver;
        private RedeliverDetailsBean redeliver_details;
        private int return_unroutable;
        private ReturnUnroutableDetailsBean return_unroutable_details;

        public int getAck() {
            return ack;
        }

        public void setAck(int ack) {
            this.ack = ack;
        }

        public AckDetailsBean getAck_details() {
            return ack_details;
        }

        public void setAck_details(AckDetailsBean ack_details) {
            this.ack_details = ack_details;
        }

        public int getConfirm() {
            return confirm;
        }

        public void setConfirm(int confirm) {
            this.confirm = confirm;
        }

        public ConfirmDetailsBean getConfirm_details() {
            return confirm_details;
        }

        public void setConfirm_details(ConfirmDetailsBean confirm_details) {
            this.confirm_details = confirm_details;
        }

        public int getDeliver() {
            return deliver;
        }

        public void setDeliver(int deliver) {
            this.deliver = deliver;
        }

        public DeliverDetailsBean getDeliver_details() {
            return deliver_details;
        }

        public void setDeliver_details(DeliverDetailsBean deliver_details) {
            this.deliver_details = deliver_details;
        }

        public int getDeliver_get() {
            return deliver_get;
        }

        public void setDeliver_get(int deliver_get) {
            this.deliver_get = deliver_get;
        }

        public DeliverGetDetailsBean getDeliver_get_details() {
            return deliver_get_details;
        }

        public void setDeliver_get_details(DeliverGetDetailsBean deliver_get_details) {
            this.deliver_get_details = deliver_get_details;
        }

        public int getDeliver_no_ack() {
            return deliver_no_ack;
        }

        public void setDeliver_no_ack(int deliver_no_ack) {
            this.deliver_no_ack = deliver_no_ack;
        }

        public DeliverNoAckDetailsBean getDeliver_no_ack_details() {
            return deliver_no_ack_details;
        }

        public void setDeliver_no_ack_details(DeliverNoAckDetailsBean deliver_no_ack_details) {
            this.deliver_no_ack_details = deliver_no_ack_details;
        }

        public int getGet() {
            return get;
        }

        public void setGet(int get) {
            this.get = get;
        }

        public GetDetailsBean getGet_details() {
            return get_details;
        }

        public void setGet_details(GetDetailsBean get_details) {
            this.get_details = get_details;
        }

        public int getGet_no_ack() {
            return get_no_ack;
        }

        public void setGet_no_ack(int get_no_ack) {
            this.get_no_ack = get_no_ack;
        }

        public GetNoAckDetailsBean getGet_no_ack_details() {
            return get_no_ack_details;
        }

        public void setGet_no_ack_details(GetNoAckDetailsBean get_no_ack_details) {
            this.get_no_ack_details = get_no_ack_details;
        }

        public int getPublish() {
            return publish;
        }

        public void setPublish(int publish) {
            this.publish = publish;
        }

        public PublishDetailsBean getPublish_details() {
            return publish_details;
        }

        public void setPublish_details(PublishDetailsBean publish_details) {
            this.publish_details = publish_details;
        }

        public int getRedeliver() {
            return redeliver;
        }

        public void setRedeliver(int redeliver) {
            this.redeliver = redeliver;
        }

        public RedeliverDetailsBean getRedeliver_details() {
            return redeliver_details;
        }

        public void setRedeliver_details(RedeliverDetailsBean redeliver_details) {
            this.redeliver_details = redeliver_details;
        }

        public int getReturn_unroutable() {
            return return_unroutable;
        }

        public void setReturn_unroutable(int return_unroutable) {
            this.return_unroutable = return_unroutable;
        }

        public ReturnUnroutableDetailsBean getReturn_unroutable_details() {
            return return_unroutable_details;
        }

        public void setReturn_unroutable_details(ReturnUnroutableDetailsBean return_unroutable_details) {
            this.return_unroutable_details = return_unroutable_details;
        }

        public static class AckDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class ConfirmDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class DeliverDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class DeliverGetDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class DeliverNoAckDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class GetDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class GetNoAckDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class PublishDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class RedeliverDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }

        public static class ReturnUnroutableDetailsBean {
            /**
             * rate : 0.0
             */

            private double rate;

            public double getRate() {
                return rate;
            }

            public void setRate(double rate) {
                this.rate = rate;
            }
        }
    }

    public static class MessagesDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MessagesReadyDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class MessagesUnacknowledgedDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class RecvOctDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }

    public static class SendOctDetailsBean {
        /**
         * rate : 0.0
         */

        private double rate;

        public double getRate() {
            return rate;
        }

        public void setRate(double rate) {
            this.rate = rate;
        }
    }
}
