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
import java.math.BigInteger;


/**
 */
public class UnsignedLongLong implements Type {

    public static final int SIZE = 8;

    private final BigInteger unsignedLongLong;

    public UnsignedLongLong(int unsignedLongLong) {
        this(BigInteger.valueOf(unsignedLongLong));
    }

    public UnsignedLongLong(BigInteger unsignedLongLong) {
        this.unsignedLongLong = unsignedLongLong;
    }

    public UnsignedLongLong(ByteBuf channelBuffer) {

        byte[] buffer = new byte[SIZE];
        channelBuffer.readBytes(buffer);

        this.unsignedLongLong = new BigInteger(buffer);

    }

    public BigInteger getUnsignedLongLong() {
        return unsignedLongLong;
    }

    public int getSize() {
        return SIZE;
    }

    public void writeTo(ByteBuf channelBuffer) {

        byte[] buffer = unsignedLongLong.toByteArray();

        channelBuffer.writeZero(SIZE - buffer.length);
        channelBuffer.writeBytes(buffer);

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }

        UnsignedLongLong that = (UnsignedLongLong) o;

        if (!unsignedLongLong.equals(that.unsignedLongLong)){
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return unsignedLongLong.hashCode();
    }

    @Override
    public String toString() {
        return unsignedLongLong.toString();
    }

}
