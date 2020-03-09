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
public class Octet implements Type {
    public static final int SIZE = 1;

    private final byte octet;

    public Octet(int i) {
        if (i < 0 || i > 255) {
            throw new IllegalArgumentException();
        }

        this.octet = (byte) i;
    }

    public Octet(byte octet) {
        this.octet = octet;
    }

    public Octet(ByteBuf channelBuffer) {
        this(channelBuffer.readByte());
    }

    public boolean getBoolean() {

        if (octet != 0 && octet != 1) {
            throw new IllegalStateException(
                    String.format("Cannot determine boolean value from octet value '%s'", octet));
        }

        return octet == 1;
    }

    public int getInt() {
        return octet;
    }

    public byte getOctet() {
        return octet;
    }

    public int getSize() {
        return SIZE;
    }

    public void writeTo(ByteBuf channelBuffer) {
        channelBuffer.writeByte(octet);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Octet octet1 = (Octet) o;

        if (octet != octet1.octet) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return (int) octet;
    }

    @Override
    public String toString() {
        return String.valueOf(octet);
    }

}
