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
package io.streamnative.pulsar.handlers.amqp.frame;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicProperty;
import io.streamnative.pulsar.handlers.amqp.frame.types.Octet;
import io.streamnative.pulsar.handlers.amqp.frame.types.Payload;
import io.streamnative.pulsar.handlers.amqp.frame.types.PropertyList;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedLongLong;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 *  ContentHeader frame (id 2).
 */
public class ContentHeaderPayload implements Payload {

    public static final Octet FRAME_ID = new Octet(2);

    private final UnsignedShort classId;
    private final UnsignedShort weight;
    private final UnsignedLongLong bodySize;
    private final PropertyList propertyList;

    public ContentHeaderPayload(ByteBuf buffer) throws Exception {

        this.classId = new UnsignedShort(buffer);
        this.weight = new UnsignedShort(buffer);
        this.bodySize = new UnsignedLongLong(buffer);

        int propertyFlags = buffer.getUnsignedShort(0);

        if (classId.equals(BasicProperty.CLASS_ID)) {
            this.propertyList = new PropertyList<BasicProperty>(buffer, BasicProperty.class, BasicProperty.values());
        } else {
            if (propertyFlags != 0) {
                throw new UnknownPropertiesException(classId);
            } else {
                this.propertyList = PropertyList.EMPTY;
            }
        }
    }

    @Override
    public Octet getFrameId() {
        return FRAME_ID;
    }

    @Override
    public String toString() {
        return "ContentHeaderPayload{" + "bodySize=" + bodySize + ", classId=" + classId
               + ", weight=" + weight + ", propertyList=" + propertyList + '}';
    }
}
