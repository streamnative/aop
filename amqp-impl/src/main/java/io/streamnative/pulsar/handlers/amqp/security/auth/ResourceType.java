package io.streamnative.pulsar.handlers.amqp.security.auth;

import java.util.HashMap;
import java.util.Locale;

/**
 * Represents a type of resource.
 *
 * Current only support namespace.
 */
public enum ResourceType {

    /**
     * Represents any ResourceType which this client cannot understand,
     * perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * A Pulsar topic.
     */
    TOPIC((byte) 1),

    /**
     * A Pulsar Namespace.
     */
    NAMESPACE((byte) 2),

    /**
     * A Pulsar tenant.
     */
    TENANT((byte) 3),

    ;


    private final byte code;

    private static final HashMap<Byte, ResourceType> CODE_TO_VALUE = new HashMap<>();

    static {
        for (ResourceType resourceType : ResourceType.values()) {
            CODE_TO_VALUE.put(resourceType.code, resourceType);
        }
    }

    /**
     * Parse the given string as an ACL resource type.
     *
     * @param str The string to parse.
     * @return The ResourceType, or UNKNOWN if the string could not be matched.
     */
    public static ResourceType fromString(String str) {
        try {
            return ResourceType.valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /**
     * Return the ResourceType with the provided code or `ResourceType.UNKNOWN` if one cannot be found.
     */
    public static ResourceType fromCode(byte code) {
        ResourceType resourceType = CODE_TO_VALUE.get(code);
        if (resourceType == null) {
            return UNKNOWN;
        }
        return resourceType;
    }

    ResourceType(byte code) {
        this.code = code;
    }

    public byte code() {
        return this.code;
    }

    public boolean isUnknown() {
        return this == UNKNOWN;
    }
}
