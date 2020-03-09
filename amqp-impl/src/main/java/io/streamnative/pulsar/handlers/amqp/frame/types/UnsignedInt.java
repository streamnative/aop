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


import io.netty.buffer.ByteBuf;

/**
 */
public class UnsignedInt implements Type {

    public static final int SIZE = 4;

    private final int unsignedInt;

    public UnsignedInt(long unsignedInt) {
        this.unsignedInt = (int) unsignedInt;
    }

    public UnsignedInt(ByteBuf channelBuffer) {
        this(channelBuffer.readUnsignedInt());
    }

    public long getUnsignedInt() {
        return unsignedInt;
    }

    public int getSize() {
        return SIZE;
    }

    public void writeTo(ByteBuf channelBuffer) {
        channelBuffer.writeBytes(new byte[] {
                (byte) (unsignedInt >>> 24),
                (byte) (unsignedInt >>> 16),
                (byte) (unsignedInt >>> 8),
                (byte) (unsignedInt) });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UnsignedInt that = (UnsignedInt) o;

        if (unsignedInt != that.unsignedInt) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public int hashCode() {
        return (unsignedInt ^ (unsignedInt >>> 32));
    }

    @Override
    public String toString() {
        return String.valueOf(unsignedInt);
    }

}
