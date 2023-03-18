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

package io.streamnative.pulsar.handlers.amqp.impl;

import io.streamnative.pulsar.handlers.amqp.AbstractAmqpMessageRouter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

/**
 * Headers message router.
 */
@Slf4j
public class HeadersMessageRouter extends AbstractAmqpMessageRouter {

    private boolean matchAny;
    private final Set<String> required = new HashSet<>();
    private final Map<String, Object> matches = new HashMap<>();

    public HeadersMessageRouter() {
        super(Type.Headers);
    }

    @Override
    public boolean isMatch(Map<String, Object> headers) {
        initMappings();
        if (headers == null) {
            return required.isEmpty() && matches.isEmpty();
        } else {
            return matchAny ? or(headers) : and(headers);
        }
    }

    private boolean and(Map<String, Object> headers) {
        if (headers.keySet().containsAll(required)) {
            for (Map.Entry<String, Object> e : matches.entrySet()) {
                if (!e.getValue().equals(headers.get(e.getKey()))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean or(Map<String, Object> headers) {
        if (required.isEmpty()) {
            return matches.isEmpty() || passesMatchesOr(headers);
        } else {
            if (!passesRequiredOr(headers)) {
                return !matches.isEmpty() && passesMatchesOr(headers);
            } else {
                return true;
            }

        }
    }

    private void initMappings() {
        for (Map.Entry<String, Object> entry : arguments.entrySet()) {
            String propertyName = entry.getKey();
            Object value = entry.getValue();
            if (isSpecial(propertyName)) {
                processSpecial(propertyName, value);
            } else if (value == null || "".equals(value)) {
                required.add(propertyName);
            } else {
                matches.put(propertyName, value);
            }
        }
    }

    private boolean passesMatchesOr(Map<String, Object> headers) {
        for (Map.Entry<String, Object> entry : matches.entrySet()) {
            if (headers.containsKey(entry.getKey())
                    && ((entry.getValue() == null && headers.get(entry.getKey()) == null)
                    || (entry.getValue().equals(headers.get(entry.getKey()))))) {
                return true;
            }
        }
        return false;
    }

    private boolean passesRequiredOr(Map<String, Object> headers) {
        for (String name : required) {
            if (headers.containsKey(name)) {
                return true;
            }
        }
        return false;
    }

    private boolean isSpecial(String key) {
        return key.startsWith("X-") || key.startsWith("x-");
    }

    private void processSpecial(String key, Object value) {
        if ("X-match".equalsIgnoreCase(key)) {
            matchAny = isAny(value);
        } else {
            log.warn("Ignoring special header: {}", key);
        }
    }

    private boolean isAny(Object value) {
        if (value instanceof String) {
            if ("any".equalsIgnoreCase((String) value)) {
                return true;
            }
            if ("all".equalsIgnoreCase((String) value)) {
                return false;
            }
        }
        log.warn("Ignoring unrecognised match type: {}", value);
        return false;
    }
}