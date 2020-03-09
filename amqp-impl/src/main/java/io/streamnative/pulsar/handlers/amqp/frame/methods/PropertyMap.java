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
package io.streamnative.pulsar.handlers.amqp.frame.methods;

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class PropertyMap<T extends Enum & Property> extends ForwardingMap<T, Object> {

    private final Map<T, Object> delegate;

    @SuppressWarnings({"unchecked"})
    public PropertyMap() {
        this.delegate = Collections.EMPTY_MAP;
    }

    public PropertyMap(Class<T> klass) {
        delegate = Maps.newEnumMap(klass);
    }

    @Override
    protected Map<T, Object> delegate() {
        return delegate;
    }

    @Override
    public Object put(T key, Object value) {

        if (!key.getValueType().equals(value.getClass())) {
            throw new IllegalArgumentException(String.format("Invalid value '%s' for key '%s'",
                    value,
                    key));
        }

        return super.put(key, value);

    }

}
