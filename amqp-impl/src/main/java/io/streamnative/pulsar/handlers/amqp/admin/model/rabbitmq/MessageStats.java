package io.streamnative.pulsar.handlers.amqp.admin.model.rabbitmq;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MessageStats {

    private long ack;
    private String ack_details;
    private long confirm;
    private String confirm_details;
    private long deliver;
    private String deliver_details;
    private long deliver_get;
    private long deliver_get_details;
    private long deliver_no_ack;
    private long deliver_no_ack_details;
    private long get;
    private long get_details;
    private long get_no_ack;
    private long get_no_ack_details;
    private long publish;
    private long publish_details;
    private long redeliver;
    private long redeliver_details;
    private long return_unroutable;
    private long return_unroutable_details;
}
