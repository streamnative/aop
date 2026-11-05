package io.streamnative.pulsar.handlers.amqp.security.auth;

import io.streamnative.pulsar.handlers.amqp.security.AmqpPrincipal;

import java.util.concurrent.CompletableFuture;

/**
 * Interface of authorizer.
 */
public interface Authorizer {

    /**
     * Check whether the specified role can perform a produce for the specified topic.
     *
     * For that the caller needs to have producer permission.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canProduceAsync(AmqpPrincipal principal, Resource resource);

    /**
     * Check whether the specified role can perform a consumer for the specified topic.
     *
     * For that the caller needs to have consumer permission.
     *
     * @param principal login info
     * @param resource resources to be authorized
     * @return a boolean to determine whether authorized or not
     */
    CompletableFuture<Boolean> canConsumeAsync(AmqpPrincipal principal, Resource resource);

}
