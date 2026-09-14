package io.streamnative.pulsar.handlers.amqp.security;

import org.apache.yetus.audience.InterfaceStability;

import java.util.HashMap;
import java.util.Locale;

/**
 * Represents an operation.
 *
 * Some operations imply other operations:
 *
 * The API for this class is still evolving and we may break compatibility in minor releases, if necessary.
 */
@InterfaceStability.Evolving
public enum AmqpOperation {
    /**
     * Represents any Operation which this client cannot understand, perhaps because this
     * client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any Operation.
     */
    ANY((byte) 1),

    /**
     * ALL operation.
     */
    ALL((byte) 2),

    /**
     * READ operation.
     */
    READ((byte) 3),

    /**
     * WRITE operation.
     */
    WRITE((byte) 4),

    /**
     * CREATE operation.
     */
    CREATE((byte) 5),

    /**
     * DELETE operation.
     */
    DELETE((byte) 6),

    /**
     * ALTER operation.
     */
    ALTER((byte) 7),

    /**
     * DESCRIBE operation.
     */
    DESCRIBE((byte) 8),

    /**
     * CLUSTER_ACTION operation.
     */
    ALTER_CONFIGS((byte) 9),

    /**
     * DESCRIBE_CONFIGS operation.
     */
    DESCRIBE_CONFIGS((byte) 10);

    // Note: we cannot have more than 30 operations without modifying the format used
    // to describe ACL operations in MetadataResponse.

    private static final HashMap<Byte, AmqpOperation> CODE_TO_VALUE = new HashMap<>();

    static {
        for (AmqpOperation operation : AmqpOperation.values()) {
            CODE_TO_VALUE.put(operation.code, operation);
        }
    }

    /**
     * Parse the given string as an operation.
     *
     * @param str    The string to parse.
     *
     * @return       The Operation, or UNKNOWN if the string could not be matched.
     */
    public static AmqpOperation fromString(String str) throws IllegalArgumentException {
        try {
            return valueOf(str.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }

    /**
     * Return the Operation with the provided code or `Operation.UNKNOWN` if one cannot be found.
     */
    public static AmqpOperation fromCode(byte code) {
        AmqpOperation operation = CODE_TO_VALUE.get(code);
        return operation == null ? UNKNOWN : operation;
    }

    private final byte code;

    AmqpOperation(byte code) {
        this.code = code;
    }

    /**
     * Return the code of this operation.
     */
    public byte code() {
        return this.code;
    }

    /**
     * Return true if this operation is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

}
