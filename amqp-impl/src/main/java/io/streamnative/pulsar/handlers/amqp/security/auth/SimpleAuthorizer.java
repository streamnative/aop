package io.streamnative.pulsar.handlers.amqp.security.auth;

import io.streamnative.pulsar.handlers.amqp.security.AmqpPrincipal;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.common.naming.TopicName;
import java.util.concurrent.CompletableFuture;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Simple authorizer.
 */
@Slf4j
public class SimpleAuthorizer implements Authorizer {

    private final AuthorizationService authorizationService;

    public SimpleAuthorizer(PulsarService pulsarService) {
        this.authorizationService = pulsarService.getBrokerService().getAuthorizationService();
    }

    @Override
    public CompletableFuture<Boolean> canProduceAsync(AmqpPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TOPIC,
                String.format("Expected resource type is TOPIC, but have [%s]", resource.getResourceType()));
        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.canProduceAsync(topicName, principal.getName(), principal.getAuthenticationData());
    }

    @Override
    public CompletableFuture<Boolean> canConsumeAsync(AmqpPrincipal principal, Resource resource) {
        checkArgument(resource.getResourceType() == ResourceType.TOPIC,
                String.format("Expected resource type is TOPIC, but have [%s]", resource.getResourceType()));

        TopicName topicName = TopicName.get(resource.getName());
        return authorizationService.canConsumeAsync(
                topicName, principal.getName(), principal.getAuthenticationData(), "");
    }
}
