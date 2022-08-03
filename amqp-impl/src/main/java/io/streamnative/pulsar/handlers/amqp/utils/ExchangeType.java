package io.streamnative.pulsar.handlers.amqp.utils;

public enum ExchangeType {

    DIRECT("direct"),
    FANOUT("fanout"),
    TOPIC("topic"),
    HEADERS("headers"),
    X_CONSISTENT_HASH("x-consistent-hash");

    private String value;

    ExchangeType(String value) {
        this.value = value;
    }

    public static ExchangeType value(String type) {
        if (type == null || type.length() == 0) {
            return null;
        }
        type = type.toLowerCase();
        switch (type) {
            case "direct":
                return DIRECT;
            case "fanout":
                return FANOUT;
            case "topic":
                return TOPIC;
            case "headers":
                return HEADERS;
            case "x-consistent-hash":
                return X_CONSISTENT_HASH;
            default:
                return null;
        }
    }

    public String getValue() {
        return value;
    }

}
