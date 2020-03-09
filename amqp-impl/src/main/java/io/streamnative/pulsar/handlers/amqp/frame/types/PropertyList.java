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

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * PropertyList.
 */
public class PropertyList<T extends Enum & Property> {

    private static final short WIDTH = 16;

    public static final PropertyList EMPTY = new PropertyList(new PropertyMap());

    private Map<T, ?> propertyMap;

    public PropertyList(PropertyMap<T> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public PropertyList(ByteBuf channelBuffer, Class<T> klass, T[] values) {

        Map<T, Object> propertyMap = new PropertyMap<T>(klass);

        final int propertyFlags = channelBuffer.readUnsignedShort();
        final List<T> stack = Lists.newArrayList();

        for (short i = 1; i < WIDTH; i++) {
            if ((propertyFlags & (1 << i)) != 0) {
                stack.add(values[WIDTH - i - 1]);
            }
        }

        for (T property :  Lists.reverse(stack)) {
            Class<? extends Type> type = property.getValueType();

            if (type == FieldTable.class) {
                propertyMap.put(property, new FieldTable(channelBuffer));
            } else if (type == Octet.class) {
                propertyMap.put(property, new Octet(channelBuffer));
            } else if (type == ShortString.class) {
                propertyMap.put(property, new ShortString(channelBuffer));
            } else if (type == Timestamp.class) {
                propertyMap.put(property, new Timestamp(channelBuffer));
            } else {
                throw new RuntimeException("");
            }

            this.propertyMap = Collections.unmodifiableMap(propertyMap);

        }
    }

    public String toString() {
        return "PropertyList{" + "propertyMap=" + propertyMap + "}";
    }

}
