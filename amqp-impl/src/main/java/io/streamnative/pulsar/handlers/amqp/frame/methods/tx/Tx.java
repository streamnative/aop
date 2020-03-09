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
package io.streamnative.pulsar.handlers.amqp.frame.methods.tx;

import io.streamnative.pulsar.handlers.amqp.frame.methods.AbstractMethod;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * Standard transactions provide so-called "1.5 phase commit". We can
 * ensure that work is never lost, but there is a chance of confirmations
 * being lost, so that messages may be resent. Applications that use
 * standard transactions must be able to detect and ignore duplicate
 * messages.
 */
public abstract class Tx extends AbstractMethod {

    public static final UnsignedShort CLASS_ID = new UnsignedShort(90);
    protected static final String CLASS_NAME = "tx";

    public final UnsignedShort getMethodClassId() {
        return CLASS_ID;
    }

    @Override
    protected final String getMethodClassName() {
        return CLASS_NAME;
    }

}
