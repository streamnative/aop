package io.streamnative.pulsar.handlers.amqp.utils;

import io.streamnative.pulsar.handlers.amqp.security.TokenAuth;
import lombok.extern.slf4j.Slf4j;

import javax.naming.AuthenticationException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Provide convenient methods to decode JWT from AMQP.
 */
@Slf4j
public class TokenUtils {

    /**
     * Decode jwt auth bytes.
     *
     * @param buf of jwt auth
     * @throws IOException
     */
    public static TokenAuth parseTokenAuthBytes(byte[] buf, String authMethod) throws AuthenticationException {
        String[] tokens;
        tokens = new String(buf, StandardCharsets.UTF_8).split("\u0000");
        if (tokens.length != 3) {
            if (log.isDebugEnabled()) {
                log.debug("Authentication data format error: mechanism={}", authMethod);
            }
            throw new AuthenticationException("Authentication data format invalid");
        }

        String password = tokens[2];

        if (password.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Authentication error, password empty: mechanism={}", authMethod);
            }
            throw new AuthenticationException("Authentication failed, password empty");
        }

        if (password.split(":").length != 2) {
            if (log.isDebugEnabled()) {
                log.debug("Authentication error, password empty: mechanism={}", authMethod);
            }
            throw new AuthenticationException("Authentication failed, token length error: password not specified");
        }

        if (authMethod.isEmpty()) {
            authMethod = "mechanism";
        }

        password = password.split(":")[1];

        return new TokenAuth(null, authMethod, password);
    }
}
