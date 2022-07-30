package io.streamnative.pulsar.handlers.amqp.utils;

public enum ExchangeType {

    DIRECT,
    FANOUT,
    TOPIC,
    HEADERS,
    X_CONSISTENT_HASH;

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


}
