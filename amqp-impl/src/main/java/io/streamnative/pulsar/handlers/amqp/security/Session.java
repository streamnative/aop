package io.streamnative.pulsar.handlers.amqp.security;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * client auth session.
 */
@Data
@AllArgsConstructor
public class Session {

    private AmqpPrincipal amqpPrincipal;

    private String clientId;
}
