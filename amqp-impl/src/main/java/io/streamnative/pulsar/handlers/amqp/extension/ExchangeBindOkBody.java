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
package io.streamnative.pulsar.handlers.amqp.extension;

import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBodyImpl;
import org.apache.qpid.server.protocol.v0_8.transport.EncodableAMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.MethodDispatcher;

public class ExchangeBindOkBody extends AMQMethodBodyImpl implements EncodableAMQDataBlock, AMQMethodBody {

    public static final int CLASS_ID =  40;
    public static final int METHOD_ID = 31;

    // Fields declared in specification

    public ExchangeBindOkBody() {
    }

    @Override
    public int getClazz() {
        return CLASS_ID;
    }

    @Override
    public int getMethod() {
        return METHOD_ID;
    }


    @Override
    protected int getBodySize() {
        int size = 0;
        return size;
    }

    @Override
    public void writeMethodPayload(QpidByteBuffer buffer) {
    }

    @Override
    public boolean execute(MethodDispatcher dispatcher, int channelId) throws QpidException {
        if (dispatcher instanceof ExtensionMethodDispatcher) {
            return ((ExtensionMethodDispatcher) dispatcher).dispatchExchangeBindOk(this, channelId);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[ExchangeBindOkBodyImpl: " + "]";
    }

}
