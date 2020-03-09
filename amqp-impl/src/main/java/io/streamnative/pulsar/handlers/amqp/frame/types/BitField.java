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
import java.util.Arrays;
import java.util.List;

/**
 * A write-only type. For reading bits see Bit.
 */
public class BitField implements Type {

    public static final int SIZE = 1;

    private final List<Bit> bits;

    public BitField(Bit... bits) {
        this(Arrays.asList(bits));
    }

    public BitField(List<Bit> bits) {
        assert bits.size() <= 8;
        this.bits = bits;
    }

    protected static byte set(byte b, boolean bit, int idx) {
         b |= (bit ? 1 : 0) << idx;
         return b;
    }

    public int getSize() {
        return SIZE;
    }

    public void writeTo(ByteBuf channelBuffer) {

        byte b = 0;

        for (short i = 0; i < bits.size(); i++) {
            b = set(b, bits.get(i).isTrue(), i);
        }

        channelBuffer.writeByte(b);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BitField bitField = (BitField) o;

        if (!bits.equals(bitField.bits)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return bits.hashCode();
    }

    @Override
    public String toString() {
        return bits.toString();
    }

}
