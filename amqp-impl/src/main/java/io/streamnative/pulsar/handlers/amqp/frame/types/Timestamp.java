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
import java.util.Date;

/**
 * Timestamp.
 */
public class Timestamp implements FieldTableValueType {

    public static final int SIZE = UnsignedLong.SIZE;
    public static final Octet IDENTIFIER = new Octet('T');

    private final Date date;

    public Timestamp(Date date) {
        this.date = date;
    }

    public Timestamp(ByteBuf channelBuffer) {
        this.date = new Date(new UnsignedLong(channelBuffer).getUnsignedLong().longValue());
    }

    public Octet getType() {
        return IDENTIFIER;
    }

    public int getSize() {
        return SIZE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Timestamp timestamp = (Timestamp) o;

        if (!date.equals(timestamp.date)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return date.hashCode();
    }

    @Override
    public String toString() {
        return "Timestamp{" + "date=" + date + '}';
    }

}
