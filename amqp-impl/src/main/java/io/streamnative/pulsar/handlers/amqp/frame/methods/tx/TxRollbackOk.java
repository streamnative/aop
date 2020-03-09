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

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * This method confirms to the client that the rollback succeeded.
 * Note that if an rollback fails, the server raises a channel exception.
 */
public final class TxRollbackOk extends Tx {

    public static final UnsignedShort METHOD_ID = new UnsignedShort(31);
    protected static final String METHOD_NAME = "rollback-ok";



    public TxRollbackOk(ByteBuf channelBuffer) {
        this();
    }

    public TxRollbackOk() {

    }

    public UnsignedShort getMethodMethodId() {
        return METHOD_ID;
    }

    @Override
    protected String getMethodMethodName() {
        return METHOD_NAME;
    }
}
