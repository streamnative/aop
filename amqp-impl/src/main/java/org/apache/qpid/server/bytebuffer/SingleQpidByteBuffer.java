/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.qpid.server.bytebuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class SingleQpidByteBuffer implements QpidByteBuffer
{
    private static final AtomicIntegerFieldUpdater<SingleQpidByteBuffer>
            DISPOSED_UPDATER = AtomicIntegerFieldUpdater.newUpdater(
            SingleQpidByteBuffer.class,
            "_disposed");

    private final int _offset;
    private final ByteBufferRef _ref;
    private volatile ByteBuffer _buffer;

    @SuppressWarnings("unused")
    private volatile int _disposed;


    SingleQpidByteBuffer(ByteBufferRef ref)
    {
        this(ref, ref.getBuffer(), 0);
    }

    private SingleQpidByteBuffer(ByteBufferRef ref, ByteBuffer buffer, int offset)
    {
        _ref = ref;
        _buffer = buffer;
        _offset = offset;
        _ref.incrementRef(capacity());
    }

    @Override
    public final boolean isDirect()
    {
        return _buffer.isDirect();
    }

    @Override
    public final short getUnsignedByte()
    {
        return (short) (((short) get()) & 0xFF);
    }

    @Override
    public final int getUnsignedShort()
    {
        return ((int) getShort()) & 0xffff;
    }

    @Override
    public final int getUnsignedShort(int pos)
    {
        return ((int) getShort(pos)) & 0xffff;
    }

    @Override
    public final long getUnsignedInt()
    {
        return ((long) getInt()) & 0xffffffffL;
    }

    @Override
    public final SingleQpidByteBuffer putUnsignedByte(final short s)
    {
        put((byte) s);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer putUnsignedShort(final int i)
    {
        putShort((short) i);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer putUnsignedInt(final long value)
    {
        putInt((int) value);
        return this;
    }

    @Override
    public final void close()
    {
        dispose();
    }

    @Override
    public final void dispose()
    {
        if (DISPOSED_UPDATER.compareAndSet(this, 0, 1))
        {
            _ref.decrementRef(capacity());
        }
        _buffer = null;
    }

    @Override
    public final InputStream asInputStream()
    {
        final QpidByteBuffer buffer = this;
        return new QpidByteBufferInputStream(buffer);
    }

    @Override
    public final long read(ScatteringByteChannel channel) throws IOException
    {
        return channel.read(getUnderlyingBuffer());
    }

    @Override
    public String toString()
    {
        return "SingleQpidByteBuffer{" +
               "_buffer=" + _buffer +
               ", _disposed=" + _disposed +
               '}';
    }

    @Override
    public final boolean hasRemaining()
    {
        return _buffer.hasRemaining();
    }

    @Override
    public boolean hasRemaining(final int atLeast)
    {
        return _buffer.remaining() >= atLeast;
    }

    @Override
    public SingleQpidByteBuffer putInt(final int index, final int value)
    {
        _buffer.putInt(index, value);
        return this;
    }

    @Override
    public SingleQpidByteBuffer putShort(final int index, final short value)
    {
        _buffer.putShort(index, value);
        return this;
    }

    @Override
    public SingleQpidByteBuffer putChar(final int index, final char value)
    {
        _buffer.putChar(index, value);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer put(final byte b)
    {
        _buffer.put(b);
        return this;
    }

    @Override
    public SingleQpidByteBuffer put(final int index, final byte b)
    {
        _buffer.put(index, b);
        return this;
    }

    @Override
    public short getShort(final int index)
    {
        return _buffer.getShort(index);
    }

    @Override
    public final SingleQpidByteBuffer mark()
    {
        _buffer.mark();
        return this;
    }

    @Override
    public final long getLong()
    {
        return _buffer.getLong();
    }

    @Override
    public SingleQpidByteBuffer putFloat(final int index, final float value)
    {
        _buffer.putFloat(index, value);
        return this;
    }

    @Override
    public double getDouble(final int index)
    {
        return _buffer.getDouble(index);
    }

    @Override
    public final boolean hasArray()
    {
        return _buffer.hasArray();
    }

    @Override
    public final double getDouble()
    {
        return _buffer.getDouble();
    }

    @Override
    public final SingleQpidByteBuffer putFloat(final float value)
    {
        _buffer.putFloat(value);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer putInt(final int value)
    {
        _buffer.putInt(value);
        return this;
    }

    @Override
    public byte[] array()
    {
        return _buffer.array();
    }

    @Override
    public final SingleQpidByteBuffer putShort(final short value)
    {
        _buffer.putShort(value);
        return this;
    }

    @Override
    public int getInt(final int index)
    {
        return _buffer.getInt(index);
    }

    @Override
    public final int remaining()
    {
        return _buffer.remaining();
    }

    @Override
    public final SingleQpidByteBuffer put(final byte[] src)
    {
        _buffer.put(src);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer put(final ByteBuffer src)
    {
        _buffer.put(src);
        return this;
    }

    @Override
    public final SingleQpidByteBuffer put(final QpidByteBuffer src)
    {
        if (src.remaining() > remaining())
        {
            throw new BufferOverflowException();
        }

        if (src instanceof SingleQpidByteBuffer)
        {
            _buffer.put(((SingleQpidByteBuffer) src).getUnderlyingBuffer());
        }
        else if (src instanceof MultiQpidByteBuffer)
        {
            for (final SingleQpidByteBuffer singleQpidByteBuffer : ((MultiQpidByteBuffer) src).getFragments())
            {
                _buffer.put(singleQpidByteBuffer.getUnderlyingBuffer());
            }
        }
        else
        {
            throw new IllegalStateException("unknown QBB implementation");
        }
        return this;
    }

    @Override
    public final SingleQpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        _buffer.get(dst, offset, length);
        return this;
    }

    @Override
    public final void copyTo(final ByteBuffer dst)
    {
        dst.put(_buffer.duplicate());
    }

    @Override
    public final void putCopyOf(final QpidByteBuffer source)
    {
        int remaining = remaining();
        int sourceRemaining = source.remaining();
        if (sourceRemaining > remaining)
        {
            throw new BufferOverflowException();
        }

        if (source instanceof SingleQpidByteBuffer)
        {
            put(((SingleQpidByteBuffer) source).getUnderlyingBuffer().duplicate());
        }
        else if (source instanceof MultiQpidByteBuffer)
        {
            for (final SingleQpidByteBuffer singleQpidByteBuffer : ((MultiQpidByteBuffer) source).getFragments())
            {
                put(singleQpidByteBuffer.getUnderlyingBuffer().duplicate());
            }
        }
        else
        {
            throw new IllegalStateException("unknown QBB implementation");
        }
    }

    @Override
    public SingleQpidByteBuffer rewind()
    {
        _buffer.rewind();
        return this;
    }

    @Override
    public SingleQpidByteBuffer clear()
    {
        _buffer.clear();
        return this;
    }

    @Override
    public SingleQpidByteBuffer putLong(final int index, final long value)
    {
        _buffer.putLong(index, value);
        return this;
    }

    @Override
    public SingleQpidByteBuffer compact()
    {
        _buffer.compact();
        return this;
    }

    @Override
    public final SingleQpidByteBuffer putDouble(final double value)
    {
        _buffer.putDouble(value);
        return this;
    }

    @Override
    public int limit()
    {
        return _buffer.limit();
    }

    @Override
    public SingleQpidByteBuffer reset()
    {
        _buffer.reset();
        return this;
    }

    @Override
    public SingleQpidByteBuffer flip()
    {
        _buffer.flip();
        return this;
    }

    @Override
    public final short getShort()
    {
        return _buffer.getShort();
    }

    @Override
    public final float getFloat()
    {
        return _buffer.getFloat();
    }

    @Override
    public SingleQpidByteBuffer limit(final int newLimit)
    {
        _buffer.limit(newLimit);
        return this;
    }

    /**
     * Method does not respect mark.
     *
     * @return SingleQpidByteBuffer
     */
    @Override
    public SingleQpidByteBuffer duplicate()
    {
        ByteBuffer buffer = _ref.getBuffer();
        if (!(_ref instanceof PooledByteBufferRef))
        {
            buffer = buffer.duplicate();
        }

        buffer.position(_offset );
        buffer.limit(_offset + _buffer.capacity());

        buffer = buffer.slice();

        buffer.limit(_buffer.limit());
        buffer.position(_buffer.position());
        return new SingleQpidByteBuffer(_ref, buffer, _offset);
    }

    @Override
    public final SingleQpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        _buffer.put(src, offset, length);
        return this;
    }

    @Override
    public long getLong(final int index)
    {
        return _buffer.getLong(index);
    }

    @Override
    public int capacity()
    {
        return _buffer.capacity();
    }

    @Override
    public char getChar(final int index)
    {
        return _buffer.getChar(index);
    }

    @Override
    public final byte get()
    {
        return _buffer.get();
    }

    @Override
    public byte get(final int index)
    {
        return _buffer.get(index);
    }

    @Override
    public final SingleQpidByteBuffer get(final byte[] dst)
    {
        _buffer.get(dst);
        return this;
    }

    @Override
    public final void copyTo(final byte[] dst)
    {
        if (remaining() < dst.length)
        {
            throw new BufferUnderflowException();
        }
        _buffer.duplicate().get(dst);
    }

    @Override
    public final SingleQpidByteBuffer putChar(final char value)
    {
        _buffer.putChar(value);
        return this;
    }

    @Override
    public SingleQpidByteBuffer position(final int newPosition)
    {
        _buffer.position(newPosition);
        return this;
    }

    @Override
    public final char getChar()
    {
        return _buffer.getChar();
    }

    @Override
    public final int getInt()
    {
        return _buffer.getInt();
    }

    @Override
    public final SingleQpidByteBuffer putLong(final long value)
    {
        _buffer.putLong(value);
        return this;
    }

    @Override
    public float getFloat(final int index)
    {
        return _buffer.getFloat(index);
    }

    @Override
    public SingleQpidByteBuffer slice()
    {
        return view(0, _buffer.remaining());
    }

    @Override
    public SingleQpidByteBuffer view(int offset, int length)
    {
        ByteBuffer buffer = _ref.getBuffer();
        if (!(_ref instanceof PooledByteBufferRef))
        {
            buffer = buffer.duplicate();
        }

        int newRemaining = Math.min(_buffer.remaining() - offset, length);

        int newPosition = _offset + _buffer.position() + offset;
        buffer.limit(newPosition + newRemaining);
        buffer.position(newPosition);

        buffer = buffer.slice();

        return new SingleQpidByteBuffer(_ref, buffer, newPosition);
    }

    @Override
    public int position()
    {
        return _buffer.position();
    }

    @Override
    public SingleQpidByteBuffer putDouble(final int index, final double value)
    {
        _buffer.putDouble(index, value);
        return this;
    }

    public ByteBuffer getUnderlyingBuffer()
    {
        return _buffer;
    }

    @Override
    public boolean isSparse()
    {
        return _ref.isSparse(QpidByteBufferFactory.getSparsityFraction());
    }
}
