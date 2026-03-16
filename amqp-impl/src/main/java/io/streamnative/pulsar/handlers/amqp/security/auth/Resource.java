package io.streamnative.pulsar.handlers.amqp.security.auth;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * The Authorization resource.
 */
@Getter
@EqualsAndHashCode
public class Resource {

    private final ResourceType resourceType;

    private final String name;

    private Resource(ResourceType resourceType, String name) {
        this.resourceType = resourceType;
        this.name = name;
    }

    public static Resource of(ResourceType resourceType, String name) {
        return new Resource(resourceType, name);
    }
}
