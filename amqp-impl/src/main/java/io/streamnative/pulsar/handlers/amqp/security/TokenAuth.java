package io.streamnative.pulsar.handlers.amqp.security;

import lombok.Data;
import lombok.NonNull;
import lombok.ToString;

/**
 * Represents an TokenAuth.
 */
@Data
@ToString
public class TokenAuth {

    private String authorizationId;

    private String authMethod;

    private String authData;

    public TokenAuth(String username, @NonNull String authMethod, @NonNull String authData) {
        this.authData = authData;
        this.authorizationId = username;
        this.authMethod = authMethod;
    }
}
