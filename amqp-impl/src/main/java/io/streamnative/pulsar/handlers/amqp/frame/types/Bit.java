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
 * A read-only type. For writing bits see BitField.
 */
public class Bit {

    private final boolean bit;

    public Bit(boolean bit) {
        this.bit = bit;
    }

    public Bit(ByteBuf channelBuffer, int idx) {
        if (idx > 0) {// return to the last byte it the idx � 0
            channelBuffer.readerIndex(channelBuffer.readerIndex() - 1);
        }

        this.bit = get(channelBuffer.readByte(), idx);
    }

    protected static boolean get(byte b, int idx) {
        assert idx <= 8;
        return (b & (1 << idx)) != 0;
    }

    public boolean isTrue() {
        return bit;
    }

    @Override
    public String toString() {
        return String.valueOf(bit);
    }

}
