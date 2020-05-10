package io.streamnative.pulsar.handlers.amqp.redirect;

public class RedirectException extends Exception {

    public RedirectException() {
        super();
    }

    public RedirectException(String message) {
        super(message);
    }

}
