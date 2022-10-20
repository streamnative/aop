/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.extension;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.EncodingUtils;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBodyImpl;
import org.apache.qpid.server.protocol.v0_8.transport.EncodableAMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.MethodDispatcher;

public class ExchangeUnbindBody extends AMQMethodBodyImpl implements EncodableAMQDataBlock, AMQMethodBody {

    public static final int CLASS_ID =  40;
    public static final int METHOD_ID = 40;

    // Fields declared in specification
    private final int ticket; // [ticket]
    private final AMQShortString destination; // [exchange]
    private final AMQShortString source; // [routingKey]
    private final AMQShortString routingKey; // [queue]
    private final FieldTable arguments; // [arguments]

    public ExchangeUnbindBody(
            int ticket,
            AMQShortString destination,
            AMQShortString source,
            AMQShortString routingKey,
            FieldTable arguments) {
        this.ticket = ticket;
        this.destination = destination;
        this.source = source;
        this.routingKey = routingKey;
        this.arguments = arguments;
    }

    @Override
    public int getClazz() {
        return CLASS_ID;
    }

    @Override
    public int getMethod() {
        return METHOD_ID;
    }


    public final int getTicket() {
        return ticket;
    }
    public final AMQShortString getDestination() {
        return destination;
    }
    public final AMQShortString getSource() {
        return source;
    }
    public final AMQShortString getRoutingKey() {
        return routingKey;
    }
    public final FieldTable getArguments() {
        return arguments;
    }

    @Override
    protected int getBodySize() {
        int size = 3;
        size += getSizeOf(destination);
        size += getSizeOf(source);
        size += getSizeOf(routingKey);
        size += getSizeOf(arguments);
        return size;
    }

    @Override
    public void writeMethodPayload(QpidByteBuffer buffer) {
        writeUnsignedShort(buffer, ticket);
        writeAMQShortString(buffer, destination);
        writeAMQShortString(buffer, source);
        writeAMQShortString(buffer, routingKey);
        writeBitfield(buffer, (byte) 0);
        writeFieldTable(buffer, arguments);
    }

    @Override
    public boolean execute(MethodDispatcher dispatcher, int channelId) throws QpidException {
        return false;
    }

    @Override
    public String toString() {
        return "[ExchangeUnbindBodyImpl: " + "exchange="
                + getDestination()
                + ", "
                + "routingKey="
                + getSource()
                + ", "
                + "queue="
                + getRoutingKey()
                + "]";
    }

    public static void process(final QpidByteBuffer buffer,
                               final ExtensionServerChannelMethodProcessor dispatcher) {

        int ticket = buffer.getUnsignedShort();
        AMQShortString destination = AMQShortString.readAMQShortString(buffer);
        AMQShortString source = AMQShortString.readAMQShortString(buffer);
        AMQShortString routingKey = AMQShortString.readAMQShortString(buffer);
        boolean nowait = (buffer.get() & 0x01) == 0x01;
        FieldTable arguments = EncodingUtils.readFieldTable(buffer);
        if (!dispatcher.ignoreAllButCloseOk()) {
            dispatcher.receiveExchangeUnbind(destination, source, routingKey, nowait,
                    FieldTable.convertToDecodedFieldTable(arguments));
        }
    }
}
