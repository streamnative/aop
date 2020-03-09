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
package io.streamnative.pulsar.handlers.amqp.frame.methods.exchange;

import io.streamnative.pulsar.handlers.amqp.frame.methods.AbstractMethod;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * Exchanges match and distribute messages across queues. Exchanges can be
 * configured in the server or created at runtime.
 */
public abstract class Exchange extends AbstractMethod {

    public static final UnsignedShort CLASS_ID = new UnsignedShort(40);
    protected static final String CLASS_NAME = "exchange";

    public final UnsignedShort getMethodClassId() {
        return CLASS_ID;
    }

    @Override
    protected final String getMethodClassName() {
        return CLASS_NAME;
    }

}
