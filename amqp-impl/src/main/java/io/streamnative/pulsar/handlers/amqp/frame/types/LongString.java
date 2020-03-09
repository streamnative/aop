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
import java.nio.charset.Charset;

/**
 * TODO: support large sizes / fetch on demand.
 */
public class LongString implements FieldTableValueType {

    public static final Octet IDENTIFIER = new Octet('S');

    private final String string;

    public LongString(String string) {
        this.string = string;
    }

    public LongString(ByteBuf channelBuffer) {
        int size = (int) new UnsignedInt(channelBuffer).getUnsignedInt();
        this.string = channelBuffer.readBytes(size).toString(Charset.forName("utf-8"));
    }

    public Octet getType() {
        return IDENTIFIER;
    }

    public int getSize() {
        return UnsignedInt.SIZE + string.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LongString that = (LongString) o;

        if (!string.equals(that.string)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return string.hashCode();
    }

    @Override
    public String toString() {
        return string;
    }

}
