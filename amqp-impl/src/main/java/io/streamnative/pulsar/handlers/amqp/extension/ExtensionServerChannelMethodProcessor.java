package io.streamnative.pulsar.handlers.amqp.extension;

import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

public interface ExtensionServerChannelMethodProcessor extends ServerChannelMethodProcessor {

    void receiveExchangeBind(AMQShortString destination, AMQShortString source,
                             AMQShortString bindingKey, boolean nowait, FieldTable fieldTable);

    void receiveExchangeUnbind(AMQShortString destination, AMQShortString source,
                             AMQShortString bindingKey, boolean nowait, FieldTable fieldTable);

}
