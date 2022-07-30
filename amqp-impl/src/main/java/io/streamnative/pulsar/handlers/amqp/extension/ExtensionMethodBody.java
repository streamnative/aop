package io.streamnative.pulsar.handlers.amqp.extension;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodDispatcher;

public interface ExtensionMethodBody extends AMQMethodBody {

    @Override
    boolean execute(MethodDispatcher methodDispatcher, int channelId) throws QpidException;

}
