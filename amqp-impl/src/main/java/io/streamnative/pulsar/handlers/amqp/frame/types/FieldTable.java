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

import static java.lang.String.format;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TODO: support pattern.
 * Field names MUST start with a letter, '$' or '#' and may continue with letters, '$' or '#', digits, or underlines,
 * to a maximum length of 128 characters.
 * TODO: decimal, signed int.
 */
public class FieldTable implements FieldTableValueType {

    public static final Octet IDENTIFIER = new Octet('F');

    private final Map<ShortString, FieldTableValueType> map;

    @SuppressWarnings({"unchecked"})
    public FieldTable() {
        this.map = Collections.EMPTY_MAP;
    }

    public FieldTable(Map<ShortString, FieldTableValueType> map) {
        this.map = map;
    }

    public FieldTable(ByteBuf channelBuffer) {
        map = new LinkedHashMap<ShortString, FieldTableValueType>();

        int size = (int) new UnsignedInt(channelBuffer).getUnsignedInt();
        ByteBuf mapBuffer = channelBuffer.readBytes(size);

        while (mapBuffer.readableBytes() > 0) {
            ShortString key = new ShortString(mapBuffer);
            FieldTableValueType value = readFieldTableValue(mapBuffer);

            if (!map.containsKey(key)) {
                map.put(key, value);
            }
        }
    }

    public Map<ShortString, FieldTableValueType> getMap() {
        return Collections.unmodifiableMap(map);
    }

    protected FieldTableValueType readFieldTableValue(ByteBuf mapBuffer) {

        Octet type = new Octet(mapBuffer);

        if (type.equals(LongString.IDENTIFIER)) {
            return new LongString(mapBuffer);
        } else if (type.equals(ShortString.IDENTIFIER)) {
            return new ShortString(mapBuffer);
        } else if (type.equals(Timestamp.IDENTIFIER)) {
            return new Timestamp(mapBuffer);
        } else {
            throw new UnsupportedOperationException(format("Field table value of '%s' is not supported (yet)", type));
        }
    }

    public Octet getType() {
        return IDENTIFIER;
    }

    public int getSize() {
        int size = UnsignedInt.SIZE;

        for (Map.Entry<ShortString, FieldTableValueType> e : map.entrySet()) {
            size += e.getKey().getSize() + e.getValue().getType().getSize() + e.getValue().getSize();
        }
        return size;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FieldTable that = (FieldTable) o;

        if (!map.equals(that.map)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return map.hashCode();
    }

    @Override
    public String toString() {
        return map.toString();
    }

}
