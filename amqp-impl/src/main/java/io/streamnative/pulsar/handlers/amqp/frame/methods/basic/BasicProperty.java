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
package io.streamnative.pulsar.handlers.amqp.frame.methods.basic;

import io.streamnative.pulsar.handlers.amqp.frame.types.FieldTable;
import io.streamnative.pulsar.handlers.amqp.frame.types.Octet;
import io.streamnative.pulsar.handlers.amqp.frame.types.Property;
import io.streamnative.pulsar.handlers.amqp.frame.types.ShortString;
import io.streamnative.pulsar.handlers.amqp.frame.types.Timestamp;
import io.streamnative.pulsar.handlers.amqp.frame.types.Type;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * Property for ContentHeader Frame.
 */
public enum BasicProperty implements Property {

    /**
     * MIME content type.
     */
    CONTENT_TYPE(ShortString.class),

    /**
     * MIME content encoding.
     */
    CONTENT_ENCODING(ShortString.class),

    /**
     * Message header field table.
     */
    HEADERS(FieldTable.class),

    /**
     * Non-persistent (1) or persistent (2).
     */
    DELIVERY_MODE(Octet.class),

    /**
     * The message priority, 0 to 9.
     */
    PRIORITY(Octet.class),

    /**
     * The application correlation identifier.
     */
    CORRELATION_ID(ShortString.class),

    /**
     * The destination to reply to.
     */
    REPLY_TO(ShortString.class),

    /**
     * Message expiration specification.
     */
    EXPIRATION(ShortString.class),

    /**
     * The application message identifier.
     */
    MESSAGE_ID(ShortString.class),

    /**
     * The message timestamp.
     */
    TIMESTAMP(Timestamp.class),

    /**
     * The message type name.
     */
    TYPE(ShortString.class),

    /**
     * The creating user id.
     */
    USER_ID(ShortString.class),

    /**
     * The creating application id.
     */
    APP_ID(ShortString.class),

    /**
     * Intra-cluster routing identifier.
     */
    CLUSTER_ID(ShortString.class);

    public static final UnsignedShort CLASS_ID = Basic.CLASS_ID;

    private final Class<? extends Type> type;

    BasicProperty(Class<? extends Type> type) {
        this.type = type;
    }

    public Class<? extends Type> getValueType() {
        return type;
    }

}
