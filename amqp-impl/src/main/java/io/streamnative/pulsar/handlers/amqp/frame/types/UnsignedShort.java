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
package io.streamnative.pulsar.handlers.amqp.frame.types;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.netty.buffer.ByteBuf;

/**
 */
public class UnsignedShort implements Type {
    public static final int SIZE = 2;
    private final int unsignedShort;

    private static final LoadingCache<Integer, UnsignedShort> cache = CacheBuilder.newBuilder()
            .maximumSize(10000)
            .build(new CacheLoader<Integer, UnsignedShort>()  {
                        public UnsignedShort load(Integer from) {
                            return new UnsignedShort(from);
                        }
                    });

    public static UnsignedShort valueOf(int unsignedShort) throws Exception {
        return cache.get(unsignedShort);
    }

    public UnsignedShort(int unsignedShort) {
        this.unsignedShort = unsignedShort;
    }

    public UnsignedShort(ByteBuf channelBuffer) {
        this(channelBuffer.readUnsignedShort());
    }

    public int getUnsignedShort() {
        return unsignedShort;
    }

    public int getSize() {
        return SIZE;
    }

    public void writeTo(ByteBuf channelBuffer) {
        channelBuffer.writeBytes(new byte[]
                { (byte) (unsignedShort >>> 8), (byte) (unsignedShort) });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnsignedShort that = (UnsignedShort) o;

        if (unsignedShort != that.unsignedShort) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return unsignedShort;
    }

    @Override
    public String toString() {
        return String.valueOf(unsignedShort);
    }

}
