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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

final class QpidByteBufferFactory
{
    private static final ByteBuffer[] EMPTY_BYTE_BUFFER_ARRAY = new ByteBuffer[0];
    private static final QpidByteBuffer EMPTY_QPID_BYTE_BUFFER = QpidByteBuffer.wrap(new byte[0]);
    private static final ThreadLocal<SingleQpidByteBuffer> _cachedBuffer = new ThreadLocal<>();
    private volatile static boolean _isPoolInitialized;
    private volatile static BufferPool _bufferPool;
    private volatile static int _pooledBufferSize;
    private volatile static double _sparsityFraction;
    private volatile static ByteBuffer _zeroed;

    static QpidByteBuffer allocate(boolean direct, int size)
    {
        return direct ? allocateDirect(size) : allocate(size);
    }

    static QpidByteBuffer allocate(int size)
    {
        return new SingleQpidByteBuffer(new NonPooledByteBufferRef(ByteBuffer.allocate(size)));
    }

    static QpidByteBuffer allocateDirect(int size)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Cannot allocate QpidByteBufferFragment with size "
                                               + size
                                               + " which is negative.");
        }

        if (_isPoolInitialized)
        {
            if (size <= _pooledBufferSize)
            {
                return allocateDirectSingle(size);
            }
            else
            {
                List<SingleQpidByteBuffer> fragments = new ArrayList<>();
                int allocatedSize = 0;
                while (size - allocatedSize >= _pooledBufferSize)
                {
                    fragments.add(allocateDirectSingle(_pooledBufferSize));
                    allocatedSize += _pooledBufferSize;
                }
                if (allocatedSize != size)
                {
                    fragments.add(allocateDirectSingle(size - allocatedSize));
                }
                return new MultiQpidByteBuffer(fragments);
            }
        }
        else
        {
            return allocate(size);
        }
    }

    static QpidByteBuffer asQpidByteBuffer(InputStream stream) throws IOException
    {
        final List<SingleQpidByteBuffer> fragments = new ArrayList<>();
        final int pooledBufferSize = getPooledBufferSize();
        byte[] transferBuf = new byte[pooledBufferSize];
        int readFragment = 0;
        int read = stream.read(transferBuf, readFragment, pooledBufferSize - readFragment);
        while (read > 0)
        {
            readFragment += read;
            if (readFragment == pooledBufferSize)
            {
                SingleQpidByteBuffer fragment = allocateDirectSingle(pooledBufferSize);
                fragment.put(transferBuf, 0, pooledBufferSize);
                fragment.flip();
                fragments.add(fragment);
                readFragment = 0;
            }
            read = stream.read(transferBuf, readFragment, pooledBufferSize - readFragment);
        }
        if (readFragment != 0)
        {
            SingleQpidByteBuffer fragment = allocateDirectSingle(readFragment);
            fragment.put(transferBuf, 0, readFragment);
            fragment.flip();
            fragments.add(fragment);
        }
        return createQpidByteBuffer(fragments);
    }

    private static QpidByteBuffer asQpidByteBuffer(final byte[] data, final int offset, final int length)
    {
        try (QpidByteBufferOutputStream outputStream = new QpidByteBufferOutputStream(true, QpidByteBuffer.getPooledBufferSize()))
        {
            outputStream.write(data, offset, length);
            return outputStream.fetchAccumulatedBuffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException("unexpected Error converting array to QpidByteBuffers", e);
        }
    }

    private static ByteBuffer[] getUnderlyingBuffers(QpidByteBuffer buffer)
    {
        if (buffer instanceof SingleQpidByteBuffer)
        {
            return new ByteBuffer[] {((SingleQpidByteBuffer) buffer).getUnderlyingBuffer()};
        }
        else if (buffer instanceof MultiQpidByteBuffer)
        {
            return ((MultiQpidByteBuffer) buffer).getUnderlyingBuffers();
        }
        else
        {
            throw new IllegalStateException("Unknown Buffer Implementation");
        }
    }

    static SSLEngineResult encryptSSL(SSLEngine engine,
                                      Collection<QpidByteBuffer> buffers,
                                      QpidByteBuffer dest) throws SSLException
    {
        if (dest instanceof SingleQpidByteBuffer)
        {
            SingleQpidByteBuffer dst = (SingleQpidByteBuffer) dest;
            final ByteBuffer[] src;
            // QPID-7447: prevent unnecessary allocations
            if (buffers.isEmpty())
            {
                src = EMPTY_BYTE_BUFFER_ARRAY;
            }
            else
            {
                List<ByteBuffer> buffers_ = new LinkedList<>();
                for (QpidByteBuffer buffer : buffers)
                {
                    Collections.addAll(buffers_, getUnderlyingBuffers(buffer));
                }
                src = buffers_.toArray(new ByteBuffer[buffers_.size()]);
            }
            return engine.wrap(src, dst.getUnderlyingBuffer());
        }
        else
        {
            throw new IllegalStateException("Expected a single fragment output buffer");
        }
    }

    static SSLEngineResult decryptSSL(final SSLEngine engine, final QpidByteBuffer src, final QpidByteBuffer dst)
            throws SSLException
    {
        if (src instanceof SingleQpidByteBuffer)
        {
            ByteBuffer underlying = ((SingleQpidByteBuffer)src).getUnderlyingBuffer();
            if (dst instanceof SingleQpidByteBuffer)
            {
                return engine.unwrap(underlying, ((SingleQpidByteBuffer) dst).getUnderlyingBuffer());
            }
            else if (dst instanceof MultiQpidByteBuffer)
            {
                return engine.unwrap(underlying, ((MultiQpidByteBuffer) dst).getUnderlyingBuffers());
            }
            else
            {
                throw new IllegalStateException("unknown QBB implementation");
            }
        }
        else
        {
            throw new IllegalStateException("Source QBB can only be single byte buffer");
        }
    }

    static QpidByteBuffer inflate(QpidByteBuffer compressedBuffer) throws IOException
    {
        if (compressedBuffer == null)
        {
            throw new IllegalArgumentException("compressedBuffer cannot be null");
        }

        boolean isDirect = compressedBuffer.isDirect();
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        List<QpidByteBuffer> uncompressedBuffers = new ArrayList<>();
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(compressedBuffer.asInputStream()))
        {
            byte[] buf = new byte[bufferSize];
            int read;
            while ((read = gzipInputStream.read(buf)) != -1)
            {
                uncompressedBuffers.add(asQpidByteBuffer(buf, 0, read));
            }
            return concatenate(uncompressedBuffers);
        }
        finally
        {
            uncompressedBuffers.forEach(QpidByteBuffer::dispose);
        }
    }

    static QpidByteBuffer deflate(QpidByteBuffer uncompressedBuffer) throws IOException
    {
        if (uncompressedBuffer == null)
        {
            throw new IllegalArgumentException("uncompressedBuffer cannot be null");
        }

        boolean isDirect = uncompressedBuffer.isDirect();
        final int bufferSize = (isDirect && _pooledBufferSize > 0) ? _pooledBufferSize : 65536;

        try (QpidByteBufferOutputStream compressedOutput = new QpidByteBufferOutputStream(isDirect, bufferSize);
             InputStream compressedInput = uncompressedBuffer.asInputStream();
             GZIPOutputStream gzipStream = new GZIPOutputStream(new BufferedOutputStream(compressedOutput,
                                                                                         bufferSize)))
        {
            byte[] buf = new byte[16384];
            int read;
            while ((read = compressedInput.read(buf)) > -1)
            {
                gzipStream.write(buf, 0, read);
            }
            gzipStream.finish();
            gzipStream.flush();
            return compressedOutput.fetchAccumulatedBuffer();
        }
    }

    static long write(GatheringByteChannel channel, Collection<QpidByteBuffer> qpidByteBuffers)
            throws IOException
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        for (QpidByteBuffer qpidByteBuffer : qpidByteBuffers)
        {
            Collections.addAll(byteBuffers, getUnderlyingBuffers(qpidByteBuffer));
        }
        return channel.write(byteBuffers.toArray(new ByteBuffer[byteBuffers.size()]));
    }

    static long write(ChannelHandlerContext channelHandlerContext, Collection<QpidByteBuffer> qpidByteBuffers)
            throws IOException
    {
        List<ByteBuffer> byteBuffers = new ArrayList<>();
        for (QpidByteBuffer qpidByteBuffer : qpidByteBuffers)
        {
            Collections.addAll(byteBuffers, getUnderlyingBuffers(qpidByteBuffer));
        }
        long writtenBytes = 0L;
        for (ByteBuffer byteBuffer : byteBuffers) {
            ByteBuf byteBuf = Unpooled.wrappedBuffer(byteBuffer);
            writtenBytes += byteBuf.readableBytes();
            channelHandlerContext.write(byteBuf);
        }
        return writtenBytes;
    }

    static long write(ChannelHandlerContext channelHandlerContext, QpidByteBuffer qpidByteBuffer)
            throws IOException
    {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(getUnderlyingBuffers(qpidByteBuffer));
        channelHandlerContext.write(byteBuf);
        return byteBuf.readableBytes();
    }

    static QpidByteBuffer wrap(ByteBuffer wrap)
    {
        return new SingleQpidByteBuffer(new NonPooledByteBufferRef(wrap));
    }

    static QpidByteBuffer wrap(byte[] data)
    {
        return wrap(ByteBuffer.wrap(data));
    }

    static QpidByteBuffer wrap(byte[] data, int offset, int length)
    {
        return wrap(ByteBuffer.wrap(data, offset, length));
    }

    static void initialisePool(int bufferSize, int maxPoolSize, double sparsityFraction)
    {
        if (_isPoolInitialized && (bufferSize != _pooledBufferSize
                                                       || maxPoolSize != _bufferPool.getMaxSize()
                                                       || sparsityFraction != _sparsityFraction))
        {
            final String errorMessage = String.format(
                    "QpidByteBuffer pool has already been initialised with bufferSize=%d, maxPoolSize=%d, and sparsityFraction=%f."
                    +
                    "Re-initialisation with different bufferSize=%d and maxPoolSize=%d is not allowed.",
                    _pooledBufferSize,
                    _bufferPool.getMaxSize(),
                    _sparsityFraction,
                    bufferSize,
                    maxPoolSize);
            throw new IllegalStateException(errorMessage);
        }
        if (bufferSize <= 0)
        {
            throw new IllegalArgumentException("Negative or zero bufferSize illegal : " + bufferSize);
        }

        _bufferPool = new BufferPool(maxPoolSize);
        _pooledBufferSize = bufferSize;
        _zeroed = ByteBuffer.allocateDirect(_pooledBufferSize);
        _sparsityFraction = sparsityFraction;
        _isPoolInitialized = true;
    }

    /**
     * Test use only
     */
    static void deinitialisePool()
    {
        if (_isPoolInitialized)
        {
            SingleQpidByteBuffer singleQpidByteBuffer = _cachedBuffer.get();
            if (singleQpidByteBuffer != null)
            {
                singleQpidByteBuffer.dispose();
                _cachedBuffer.remove();
            }
            _bufferPool = null;
            _pooledBufferSize = -1;
            _isPoolInitialized = false;
            _sparsityFraction = 1.0;
            _zeroed = null;
        }
    }


    static void returnToPool(final ByteBuffer buffer)
    {
        buffer.clear();
        if (_isPoolInitialized)
        {
            final ByteBuffer duplicate = _zeroed.duplicate();
            duplicate.limit(buffer.capacity());
            buffer.put(duplicate);
            _bufferPool.returnBuffer(buffer);
        }
    }

    static double getSparsityFraction()
    {
        return _sparsityFraction;
    }

    static int getPooledBufferSize()
    {
        return _pooledBufferSize;
    }

    static long getAllocatedDirectMemorySize()
    {
        return (long)_pooledBufferSize * getNumberOfBuffersInUse();
    }

    static int getNumberOfBuffersInUse()
    {
        return PooledByteBufferRef.getActiveBufferCount();
    }

    static int getNumberOfBuffersInPool()
    {
        return _bufferPool.size();
    }

    static long getPooledBufferDisposalCounter()
    {
        return PooledByteBufferRef.getDisposalCounter();
    }

    static QpidByteBuffer reallocateIfNecessary(QpidByteBuffer data)
    {
        if (data != null && data.isDirect() && data.isSparse())
        {
            QpidByteBuffer newBuf = allocateDirect(data.remaining());
            newBuf.put(data);
            newBuf.flip();
            data.dispose();
            return newBuf;
        }
        else
        {
            return data;
        }
    }

    static QpidByteBuffer concatenate(List<QpidByteBuffer> buffers)
    {
        final List<SingleQpidByteBuffer> fragments = new ArrayList<>(buffers.size());
        for (QpidByteBuffer buffer : buffers)
        {
            if (buffer instanceof SingleQpidByteBuffer)
            {
                if (buffer.hasRemaining())
                {
                    fragments.add((SingleQpidByteBuffer) buffer.slice());
                }
            }
            else if (buffer instanceof MultiQpidByteBuffer)
            {
                for (final SingleQpidByteBuffer fragment : ((MultiQpidByteBuffer) buffer).getFragments())
                {
                    if (fragment.hasRemaining())
                    {
                        fragments.add(fragment.slice());
                    }
                }
            }
            else
            {
                throw new IllegalStateException("unknown QBB implementation");
            }
        }
        return createQpidByteBuffer(fragments);
    }

    static QpidByteBuffer createQpidByteBuffer(final List<SingleQpidByteBuffer> fragments)
    {
        if (fragments.size() == 0)
        {
            return emptyQpidByteBuffer();
        }
        else if (fragments.size() == 1)
        {
            return fragments.get(0);
        }
        else
        {
            return new MultiQpidByteBuffer(fragments);
        }
    }

    static QpidByteBuffer concatenate(QpidByteBuffer... buffers)
    {
        return concatenate(Arrays.asList(buffers));
    }

    static QpidByteBuffer emptyQpidByteBuffer()
    {
        return EMPTY_QPID_BYTE_BUFFER.duplicate();
    }

    static ThreadFactory createQpidByteBufferTrackingThreadFactory(ThreadFactory factory)
    {
        return r -> factory.newThread(() -> {
            try
            {
                r.run();
            }
            finally
            {
                final SingleQpidByteBuffer cachedThreadLocalBuffer = _cachedBuffer.get();
                if (cachedThreadLocalBuffer != null)
                {
                    cachedThreadLocalBuffer.dispose();
                    _cachedBuffer.remove();
                }
            }
        });
    }

    private static SingleQpidByteBuffer allocateDirectSingle(int size)
    {
        if (size < 0)
        {
            throw new IllegalArgumentException("Cannot allocate SingleQpidByteBuffer with size "
                                               + size
                                               + " which is negative.");
        }

        final ByteBufferRef ref;
        if (_isPoolInitialized && _pooledBufferSize >= size)
        {
            if (_pooledBufferSize == size)
            {
                ByteBuffer buf = _bufferPool.getBuffer();
                if (buf == null)
                {
                    buf = ByteBuffer.allocateDirect(size);
                }
                ref = new PooledByteBufferRef(buf);
            }
            else
            {
                SingleQpidByteBuffer buf = _cachedBuffer.get();
                if (buf == null || buf.remaining() < size)
                {
                    if (buf != null)
                    {
                        buf.dispose();
                    }
                    buf = allocateDirectSingle(_pooledBufferSize);
                    _cachedBuffer.set(buf);
                }
                SingleQpidByteBuffer rVal = buf.view(0, size);
                buf.position(buf.position() + size);

                return rVal;
            }
        }
        else
        {
            ref = new NonPooledByteBufferRef(ByteBuffer.allocateDirect(size));
        }
        return new SingleQpidByteBuffer(ref);
    }

}
