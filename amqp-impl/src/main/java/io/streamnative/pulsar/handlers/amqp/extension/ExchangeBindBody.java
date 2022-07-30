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

public class ExchangeBindBody extends AMQMethodBodyImpl implements EncodableAMQDataBlock, AMQMethodBody
{

    public static final int CLASS_ID =  40;
    public static final int METHOD_ID = 22;

    // Fields declared in specification
    private final AMQShortString _exchange; // [exchange]
    private final AMQShortString _routingKey; // [routingKey]
    private final AMQShortString _queue; // [queue]

    public ExchangeBindBody(
            AMQShortString exchange,
            AMQShortString routingKey,
            AMQShortString queue
                            )
    {
        _exchange = exchange;
        _routingKey = routingKey;
        _queue = queue;
    }

    @Override
    public int getClazz()
    {
        return CLASS_ID;
    }

    @Override
    public int getMethod()
    {
        return METHOD_ID;
    }

    public final AMQShortString getExchange()
    {
        return _exchange;
    }
    public final AMQShortString getRoutingKey()
    {
        return _routingKey;
    }
    public final AMQShortString getQueue()
    {
        return _queue;
    }

    @Override
    protected int getBodySize()
    {
        int size = 0;
        size += getSizeOf( _exchange );
        size += getSizeOf( _routingKey );
        size += getSizeOf( _queue );
        return size;
    }

    @Override
    public void writeMethodPayload(QpidByteBuffer buffer)
    {
        writeAMQShortString( buffer, _exchange );
        writeAMQShortString( buffer, _routingKey );
        writeAMQShortString( buffer, _queue );
    }

    @Override
    public boolean execute(MethodDispatcher dispatcher, int channelId) throws QpidException
	{
        if (dispatcher instanceof ExtensionMethodDispatcher) {
            return ((ExtensionMethodDispatcher) dispatcher).dispatchExchangeBind(this, channelId);
        } else {
            return false;
        }
	}

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder("[ExchangeBindBodyImpl: ");
        buf.append( "exchange=" );
        buf.append(  getExchange() );
        buf.append( ", " );
        buf.append( "routingKey=" );
        buf.append(  getRoutingKey() );
        buf.append( ", " );
        buf.append( "queue=" );
        buf.append(  getQueue() );
        buf.append("]");
        return buf.toString();
    }

    public static void process(final QpidByteBuffer buffer,
                               final ExtensionServerChannelMethodProcessor dispatcher)
    {

        int ticket = buffer.getUnsignedShort();
        AMQShortString destination = AMQShortString.readAMQShortString(buffer);
        AMQShortString source = AMQShortString.readAMQShortString(buffer);
        AMQShortString bindingKey = AMQShortString.readAMQShortString(buffer);
        boolean nowait = (buffer.get() & 0x01) == 0x01;
        FieldTable arguments = EncodingUtils.readFieldTable(buffer);
        if(!dispatcher.ignoreAllButCloseOk())
        {
            dispatcher.receiveExchangeBind(destination, source, bindingKey, nowait, FieldTable.convertToDecodedFieldTable(arguments));
        }
    }
}
