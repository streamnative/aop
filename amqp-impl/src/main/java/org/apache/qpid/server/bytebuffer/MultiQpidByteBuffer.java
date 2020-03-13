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
package org.apache.qpid.server.bytebuffer;

import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.ScatteringByteChannel;
import java.util.ArrayList;
import java.util.List;

public class MultiQpidByteBuffer implements QpidByteBuffer
{
    private final SingleQpidByteBuffer[] _fragments;
    private volatile int _resetFragmentIndex = -1;

    private MultiQpidByteBuffer(final SingleQpidByteBuffer... fragments)
    {
        if (fragments == null)
        {
            throw new IllegalArgumentException();
        }
        _fragments = fragments;
    }

    MultiQpidByteBuffer(final List<SingleQpidByteBuffer> fragments)
    {
        if (fragments == null)
        {
            throw new IllegalArgumentException();
        }
        _fragments = fragments.toArray(new SingleQpidByteBuffer[fragments.size()]);
    }

    //////////////////
    // Absolute puts
    //////////////////

    @Override
    public QpidByteBuffer put(final int index, final byte b)
    {
        return put(index, new byte[]{b});
    }

    @Override
    public QpidByteBuffer putShort(final int index, final short value)
    {
        byte[] valueArray = Shorts.toByteArray(value);
        return put(index, valueArray);
    }

    @Override
    public QpidByteBuffer putChar(final int index, final char value)
    {
        byte[] valueArray = Chars.toByteArray(value);
        return put(index, valueArray);
    }

    @Override
    public QpidByteBuffer putInt(final int index, final int value)
    {
        byte[] valueArray = Ints.toByteArray(value);
        return put(index, valueArray);
    }

    @Override
    public QpidByteBuffer putLong(final int index, final long value)
    {
        byte[] valueArray = Longs.toByteArray(value);
        return put(index, valueArray);
    }

    @Override
    public QpidByteBuffer putFloat(final int index, final float value)
    {
        int intValue = Float.floatToRawIntBits(value);
        return putInt(index, intValue);
    }

    @Override
    public QpidByteBuffer putDouble(final int index, final double value)
    {
        long longValue = Double.doubleToRawLongBits(value);
        return putLong(index, longValue);
    }

    private QpidByteBuffer put(final int index, final byte[] src)
    {
        final int valueWidth = src.length;
        if (index < 0 || index + valueWidth > limit())
        {
            throw new IndexOutOfBoundsException(String.format("index %d is out of bounds [%d, %d)", index, 0, limit()));
        }

        int written = 0;
        int bytesToSkip = index;
        for (int i = 0; i < _fragments.length && written != valueWidth; i++)
        {
            final SingleQpidByteBuffer buffer = _fragments[i];
            final int limit = buffer.limit();
            boolean isLastFragmentToConsider = valueWidth + bytesToSkip - written <= limit;
            if (!isLastFragmentToConsider && limit != buffer.capacity())
            {
                throw new IllegalStateException(String.format("Unexpected limit %d on fragment %d", limit, i));
            }

            if (bytesToSkip >= limit)
            {
                bytesToSkip -= limit;
            }
            else
            {
                final int bytesToCopy = Math.min(limit - bytesToSkip, valueWidth - written);
                final int originalPosition = buffer.position();
                buffer.position(bytesToSkip);
                buffer.put(src, written, bytesToCopy);
                buffer.position(originalPosition);
                written += bytesToCopy;
                bytesToSkip = 0;
            }
        }
        if (valueWidth != written)
        {
            throw new BufferOverflowException();
        }
        return this;
    }

    ////////////////
    // Relative Puts
    ////////////////

    @Override
    public final QpidByteBuffer put(final byte b)
    {
        return put(new byte[]{b});
    }

    @Override
    public final QpidByteBuffer putUnsignedByte(final short s)
    {
        put((byte) s);
        return this;
    }

    @Override
    public final QpidByteBuffer putShort(final short value)
    {
        byte[] valueArray = Shorts.toByteArray(value);
        return put(valueArray);
    }

    @Override
    public final QpidByteBuffer putUnsignedShort(final int i)
    {
        putShort((short) i);
        return this;
    }

    @Override
    public final QpidByteBuffer putChar(final char value)
    {
        byte[] valueArray = Chars.toByteArray(value);
        return put(valueArray);
    }

    @Override
    public final QpidByteBuffer putInt(final int value)
    {
        byte[] valueArray = Ints.toByteArray(value);
        return put(valueArray);
    }

    @Override
    public final QpidByteBuffer putUnsignedInt(final long value)
    {
        putInt((int) value);
        return this;
    }

    @Override
    public final QpidByteBuffer putLong(final long value)
    {
        byte[] valueArray = Longs.toByteArray(value);
        return put(valueArray);
    }

    @Override
    public final QpidByteBuffer putFloat(final float value)
    {
        int intValue = Float.floatToRawIntBits(value);
        return putInt(intValue);
    }

    @Override
    public final QpidByteBuffer putDouble(final double value)
    {
        long longValue = Double.doubleToRawLongBits(value);
        return putLong(longValue);
    }

    @Override
    public final QpidByteBuffer put(byte[] src)
    {
        return put(src, 0, src.length);
    }

    @Override
    public final QpidByteBuffer put(final byte[] src, final int offset, final int length)
    {
        if (!hasRemaining(length))
        {
            throw new BufferOverflowException();
        }

        int written = 0;
        for (int i = 0; i < _fragments.length && written != length; i++)
        {
            final SingleQpidByteBuffer buffer = _fragments[i];
            int bytesToWrite = Math.min(buffer.remaining(), length - written);
            buffer.put(src, offset + written, bytesToWrite);
            written += bytesToWrite;
        }
        if (written != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.", written, length));
        }
        return this;
    }

    @Override
    public final QpidByteBuffer put(final ByteBuffer src)
    {
        final int valueWidth = src.remaining();
        if (!hasRemaining(valueWidth))
        {
            throw new BufferOverflowException();
        }

        int written = 0;
        for (int i = 0; i < _fragments.length && written != valueWidth; i++)
        {
            final SingleQpidByteBuffer dstFragment = _fragments[i];
            if (dstFragment.hasRemaining())
            {
                final int srcFragmentRemaining = src.remaining();
                final int dstFragmentRemaining = dstFragment.remaining();
                if (dstFragmentRemaining >= srcFragmentRemaining)
                {
                    dstFragment.put(src);
                    written += srcFragmentRemaining;
                }
                else
                {
                    int srcOriginalLimit = src.limit();
                    src.limit(src.position() + dstFragmentRemaining);
                    dstFragment.put(src);
                    src.limit(srcOriginalLimit);
                    written += dstFragmentRemaining;
                }
            }
        }
        if (written != valueWidth)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.", written, valueWidth));
        }
        return this;
    }

    @Override
    public final QpidByteBuffer put(final QpidByteBuffer qpidByteBuffer)
    {
        final int valueWidth = qpidByteBuffer.remaining();
        if (!hasRemaining(valueWidth))
        {
            throw new BufferOverflowException();
        }

        int written = 0;
        final SingleQpidByteBuffer[] fragments;
        if (qpidByteBuffer instanceof SingleQpidByteBuffer)
        {
            final SingleQpidByteBuffer srcFragment = (SingleQpidByteBuffer) qpidByteBuffer;
            for (int i = 0; i < _fragments.length && written != valueWidth; i++)
            {
                final SingleQpidByteBuffer dstFragment = _fragments[i];
                if (dstFragment.hasRemaining())
                {
                    final int dstFragmentRemaining = dstFragment.remaining();
                    if (dstFragmentRemaining >= valueWidth)
                    {
                        dstFragment.put(srcFragment);
                        written += valueWidth;
                    }
                    else
                    {
                        int srcOriginalLimit = srcFragment.limit();
                        srcFragment.limit(srcFragment.position() + dstFragmentRemaining);
                        dstFragment.put(srcFragment);
                        srcFragment.limit(srcOriginalLimit);
                        written += dstFragmentRemaining;
                    }
                }
            }
        }
        else if (qpidByteBuffer instanceof MultiQpidByteBuffer)
        {
            fragments = ((MultiQpidByteBuffer) qpidByteBuffer)._fragments;
            int i = 0;
            for (int i1 = 0; i1 < fragments.length; i1++)
            {
                final SingleQpidByteBuffer srcFragment = fragments[i1];
                for (; i < _fragments.length; i++)
                {
                    final SingleQpidByteBuffer dstFragment = _fragments[i];
                    if (dstFragment.hasRemaining())
                    {
                        final int srcFragmentRemaining = srcFragment.remaining();
                        final int dstFragmentRemaining = dstFragment.remaining();
                        if (dstFragmentRemaining >= srcFragmentRemaining)
                        {
                            dstFragment.put(srcFragment);
                            written += srcFragmentRemaining;
                            break;
                        }
                        else
                        {
                            int srcOriginalLimit = srcFragment.limit();
                            srcFragment.limit(srcFragment.position() + dstFragmentRemaining);
                            dstFragment.put(srcFragment);
                            srcFragment.limit(srcOriginalLimit);
                            written += dstFragmentRemaining;
                        }
                    }
                }
            }
        }
        else
        {
            throw new IllegalStateException("unknown QBB implementation");
        }

        if (written != valueWidth)
        {
            throw new IllegalStateException(String.format("Unexpectedly only wrote %d of %d bytes.",
                                                          written,
                                                          valueWidth));
        }
        return this;
    }

    ///////////////////
    // Absolute Gets
    ///////////////////

    @Override
    public byte get(final int index)
    {
        final byte[] byteArray = getByteArray(index, 1);
        return byteArray[0];
    }

    @Override
    public short getShort(final int index)
    {
        final byte[] byteArray = getByteArray(index, 2);
        return Shorts.fromByteArray(byteArray);
    }

    @Override
    public final int getUnsignedShort(int index)
    {
        return ((int) getShort(index)) & 0xFFFF;
    }

    @Override
    public char getChar(final int index)
    {
        final byte[] byteArray = getByteArray(index, 2);
        return Chars.fromByteArray(byteArray);
    }

    @Override
    public int getInt(final int index)
    {
        final byte[] byteArray = getByteArray(index, 4);
        return Ints.fromByteArray(byteArray);
    }

    @Override
    public long getLong(final int index)
    {
        final byte[] byteArray = getByteArray(index, 8);
        return Longs.fromByteArray(byteArray);
    }

    @Override
    public float getFloat(final int index)
    {
        final int intValue = getInt(index);
        return Float.intBitsToFloat(intValue);
    }

    @Override
    public double getDouble(final int index)
    {
        final long longValue = getLong(index);
        return Double.longBitsToDouble(longValue);
    }

    private byte[] getByteArray(final int index, final int length)
    {
        if (index < 0 || index + length > limit())
        {
            throw new IndexOutOfBoundsException(String.format("%d bytes at index %d do not fit into bounds [%d, %d)", length, index, 0, limit()));
        }

        byte[] value = new byte[length];

        int consumed = 0;
        int bytesToSkip = index;
        for (int i = 0; i < _fragments.length && consumed != length; i++)
        {
            final SingleQpidByteBuffer buffer = _fragments[i];
            final int limit = buffer.limit();
            boolean isLastFragmentToConsider = length + bytesToSkip - consumed <= limit;
            if (!isLastFragmentToConsider && limit != buffer.capacity())
            {
                throw new IllegalStateException(String.format("Unexpectedly limit %d on fragment %d.", limit, i));
            }

            if (bytesToSkip >= limit)
            {
                bytesToSkip -= limit;
            }
            else
            {
                final int bytesToCopy = Math.min(limit - bytesToSkip, length - consumed);
                final int originalPosition = buffer.position();
                buffer.position(bytesToSkip);
                buffer.get(value, consumed, bytesToCopy);
                buffer.position(originalPosition);
                consumed += bytesToCopy;
                bytesToSkip = 0;
            }
        }
        if (consumed != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only consumed %d of %d bytes.", consumed, length));
        }
        return value;
    }

    //////////////////
    // Relative Gets
    //////////////////

    @Override
    public final byte get()
    {
        byte[] value = new byte[1];
        get(value, 0, 1);
        return value[0];
    }

    @Override
    public final short getUnsignedByte()
    {
        return (short) (get() & 0xFF);
    }

    @Override
    public final short getShort()
    {
        byte[] value = new byte[2];
        get(value, 0, value.length);
        return Shorts.fromByteArray(value);
    }

    @Override
    public final int getUnsignedShort()
    {
        return ((int) getShort()) & 0xFFFF;
    }

    @Override
    public final char getChar()
    {
        byte[] value = new byte[2];
        get(value, 0, value.length);
        return Chars.fromByteArray(value);
    }

    @Override
    public final int getInt()
    {
        byte[] value = new byte[4];
        get(value, 0, value.length);
        return Ints.fromByteArray(value);
    }

    @Override
    public final long getUnsignedInt()
    {
        return ((long) getInt()) & 0xFFFFFFFFL;
    }

    @Override
    public final long getLong()
    {
        byte[] value = new byte[8];
        get(value, 0, value.length);
        return Longs.fromByteArray(value);
    }

    @Override
    public final float getFloat()
    {
        final int intValue = getInt();
        return Float.intBitsToFloat(intValue);
    }

    @Override
    public final double getDouble()
    {
        final long longValue = getLong();
        return Double.longBitsToDouble(longValue);
    }

    @Override
    public final QpidByteBuffer get(final byte[] dst)
    {
        return get(dst, 0, dst.length);
    }

    @Override
    public final QpidByteBuffer get(final byte[] dst, final int offset, final int length)
    {
        if (!hasRemaining(length))
        {
            throw new BufferUnderflowException();
        }

        int consumed = 0;
        for (int i = 0; i < _fragments.length && consumed != length; i++)
        {
            final SingleQpidByteBuffer buffer = _fragments[i];
            int bytesToCopy = Math.min(buffer.remaining(), length - consumed);
            buffer.get(dst, offset + consumed, bytesToCopy);
            consumed += bytesToCopy;
        }
        if (consumed != length)
        {
            throw new IllegalStateException(String.format("Unexpectedly only consumed %d of %d bytes.", consumed, length));
        }
        return this;
    }

    ///////////////
    // Other stuff
    ////////////////

    @Override
    public final void copyTo(final byte[] dst)
    {
        final int remaining = remaining();
        if (remaining < dst.length)
        {
            throw new BufferUnderflowException();
        }
        if (remaining > dst.length)
        {
            throw new BufferOverflowException();
        }
        int offset = 0;
        for (SingleQpidByteBuffer fragment : _fragments)
        {
            final int length = Math.min(fragment.remaining(), dst.length - offset);
            fragment.getUnderlyingBuffer().duplicate().get(dst, offset, length);
            offset += length;
        }
    }

    @Override
    public final void copyTo(final ByteBuffer dst)
    {
        if (dst.remaining() < remaining())
        {
            throw new BufferOverflowException();
        }
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            dst.put(fragment.getUnderlyingBuffer().duplicate());
        }
    }

    @Override
    public final void putCopyOf(final QpidByteBuffer qpidByteBuffer)
    {
        int sourceRemaining = qpidByteBuffer.remaining();
        if (!hasRemaining(sourceRemaining))
        {
            throw new BufferOverflowException();
        }
        if (qpidByteBuffer instanceof MultiQpidByteBuffer)
        {
            MultiQpidByteBuffer source = (MultiQpidByteBuffer) qpidByteBuffer;
            for (int i = 0, fragmentsSize = source._fragments.length; i < fragmentsSize; i++)
            {
                final SingleQpidByteBuffer srcFragment = source._fragments[i];
                put(srcFragment.getUnderlyingBuffer().duplicate());
            }
        }
        else if (qpidByteBuffer instanceof SingleQpidByteBuffer)
        {
            SingleQpidByteBuffer source = (SingleQpidByteBuffer) qpidByteBuffer;
            put(source.getUnderlyingBuffer().duplicate());
        }
        else
        {
            throw new IllegalStateException("unknown QBB implementation");
        }
    }

    @Override
    public final boolean isDirect()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            if (!fragment.isDirect())
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public final void close()
    {
        dispose();
    }

    @Override
    public final void dispose()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            fragment.dispose();
        }
    }

    @Override
    public final InputStream asInputStream()
    {
        return new QpidByteBufferInputStream(this);
    }

    @Override
    public final long read(ScatteringByteChannel channel) throws IOException
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[_fragments.length];
        for (int i = 0; i < byteBuffers.length; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            byteBuffers[i] = fragment.getUnderlyingBuffer();
        }
        return channel.read(byteBuffers);
    }

    @Override
    public String toString()
    {
        return "QpidByteBuffer{" + _fragments.length + " fragments}";
    }

    @Override
    public QpidByteBuffer reset()
    {
        if (_resetFragmentIndex < 0)
        {
            throw new InvalidMarkException();
        }
        final SingleQpidByteBuffer fragment = _fragments[_resetFragmentIndex];
        fragment.reset();
        for (int i = _resetFragmentIndex + 1, size = _fragments.length; i < size; ++i)
        {
            _fragments[i].position(0);
        }
        return this;
    }

    @Override
    public QpidByteBuffer rewind()
    {
        _resetFragmentIndex = -1;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            fragment.rewind();
        }
        return this;
    }

    @Override
    public final boolean hasArray()
    {
        return false;
    }

    @Override
    public byte[] array()
    {
        throw new UnsupportedOperationException("This QpidByteBuffer is not backed by an array.");
    }

    @Override
    public QpidByteBuffer clear()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            _fragments[i].clear();
        }
        return this;
    }

    @Override
    public QpidByteBuffer compact()
    {
        int position = position();
        int limit = limit();
        if (position != 0)
        {
            int dstPos = 0;
            for (int srcPos = position; srcPos < limit; ++srcPos, ++dstPos)
            {
                put(dstPos, get(srcPos));
            }
            position(dstPos);
            limit(capacity());
        }
        _resetFragmentIndex = -1;
        return this;
    }

    @Override
    public int position()
    {
        int totalPosition = 0;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            totalPosition += fragment.position();
            if (fragment.position() != fragment.limit())
            {
                break;
            }
        }
        return totalPosition;
    }

    @Override
    public QpidByteBuffer position(int newPosition)
    {
        if (newPosition < 0 || newPosition > limit())
        {
            throw new IllegalArgumentException(String.format("new position %d is out of bounds [%d, %d)", newPosition, 0, limit()));
        }
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            final int fragmentLimit = fragment.limit();
            if (newPosition <= fragmentLimit)
            {
                fragment.position(newPosition);
                newPosition = 0;
            }
            else
            {
                if (fragmentLimit != fragment.capacity())
                {
                    throw new IllegalStateException(String.format("QBB Fragment %d has limit %d != capacity %d",
                                                                  i,
                                                                  fragmentLimit,
                                                                  fragment.capacity()));
                }
                fragment.position(fragmentLimit);
                newPosition -= fragmentLimit;
            }
        }
        return this;
    }

    @Override
    public int limit()
    {
        int totalLimit = 0;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            final int fragmentLimit = fragment.limit();
            totalLimit += fragmentLimit;
            if (fragmentLimit != fragment.capacity())
            {
                break;
            }
        }

        return totalLimit;
    }

    @Override
    public QpidByteBuffer limit(int newLimit)
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            final int fragmentCapacity = fragment.capacity();
            final int fragmentLimit = Math.min(newLimit, fragmentCapacity);
            fragment.limit(fragmentLimit);
            newLimit -= fragmentLimit;
        }
        return this;
    }

    @Override
    public final QpidByteBuffer mark()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            if (fragment.position() != fragment.limit())
            {
                fragment.mark();
                _resetFragmentIndex = i;
                return this;
            }
        }
        _resetFragmentIndex = _fragments.length - 1;
        _fragments[_resetFragmentIndex].mark();
        return this;
    }

    @Override
    public final int remaining()
    {
        int remaining = 0;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            remaining += fragment.remaining();
        }
        return remaining;
    }

    @Override
    public final boolean hasRemaining()
    {
        return hasRemaining(1);
    }

    @Override
    public final boolean hasRemaining(int atLeast)
    {
        if (atLeast == 0)
        {
            return true;
        }
        int remaining = 0;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            remaining += fragment.remaining();
            if (remaining >= atLeast)
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public QpidByteBuffer flip()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            fragment.flip();
        }
        return this;
    }

    @Override
    public int capacity()
    {
        int totalCapacity = 0;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            totalCapacity += _fragments[i].capacity();
        }
        return totalCapacity;
    }

    @Override
    public QpidByteBuffer duplicate()
    {
        final SingleQpidByteBuffer[] fragments = new SingleQpidByteBuffer[_fragments.length];
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            fragments[i] =_fragments[i].duplicate();
        }
        MultiQpidByteBuffer duplicate = new MultiQpidByteBuffer(fragments);
        duplicate._resetFragmentIndex = _resetFragmentIndex;
        return duplicate;
    }

    @Override
    public QpidByteBuffer slice()
    {
        return view(0, remaining());
    }

    @Override
    public QpidByteBuffer view(int offset, int length)
    {
        if (offset + length > remaining())
        {
            throw new IllegalArgumentException(String.format("offset: %d, length: %d, remaining: %d", offset, length, remaining()));
        }

        final List<SingleQpidByteBuffer> fragments = new ArrayList<>(_fragments.length);

        boolean firstFragmentToBeConsidered = true;
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize && length > 0; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            if (fragment.hasRemaining())
            {
                if (!firstFragmentToBeConsidered && fragment.position() != 0)
                {
                    throw new IllegalStateException(String.format("Unexpectedly position %d on fragment %d.", fragment.position(), i));
                }
                firstFragmentToBeConsidered = false;
                final int fragmentRemaining = fragment.remaining();
                if (fragmentRemaining > offset)
                {
                    final int fragmentViewLength = Math.min(fragmentRemaining - offset, length);
                    fragments.add(fragment.view(offset, fragmentViewLength));
                    length -= fragmentViewLength;
                    offset = 0;
                }
                else
                {
                    offset -= fragmentRemaining;
                }
            }
        }

        return QpidByteBufferFactory.createQpidByteBuffer(fragments);
    }

    @Override
    public boolean isSparse()
    {
        for (int i = 0, fragmentsSize = _fragments.length; i < fragmentsSize; i++)
        {
            final SingleQpidByteBuffer fragment = _fragments[i];
            if (fragment.isSparse())
            {
                return true;
            }
        }
        return false;
    }

    SingleQpidByteBuffer[] getFragments()
    {
        return _fragments;
    }

    public ByteBuffer[] getUnderlyingBuffers()
    {
        ByteBuffer[] byteBuffers = new ByteBuffer[_fragments.length];
        for (int i = 0; i < _fragments.length; i++)
        {
            byteBuffers[i] = _fragments[i].getUnderlyingBuffer();
        }
        return byteBuffers;

    }
}
