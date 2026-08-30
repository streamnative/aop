package io.streamnative.pulsar.handlers.amqp.security;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;

import java.security.Principal;

/**
 * Store client login info.
 */
@Getter
@ToString
@AllArgsConstructor
public class AmqpPrincipal implements Principal {

    public static final String USER_TYPE = "User";

    private final String principalType;

    /**
     * Pulsar role.
     */
    private final String name;

    /**
     * Pulsar Tenant Specs.
     * It can be "tenant" or "tenant/namespace"
     */
    private final String tenantSpec;

    private final AuthenticationDataSource authenticationData;
}
