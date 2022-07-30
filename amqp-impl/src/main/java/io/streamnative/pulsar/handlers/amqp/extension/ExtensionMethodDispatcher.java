package io.streamnative.pulsar.handlers.amqp.extension;

import org.apache.qpid.server.protocol.v0_8.transport.MethodDispatcher;

public interface ExtensionMethodDispatcher extends MethodDispatcher {

    boolean dispatchExchangeBind(ExchangeBindBody exchangeBindBody, int channelId);

    boolean dispatchExchangeBindOk(ExchangeBindOkBody exchangeBindOkBody, int channelId);

}
