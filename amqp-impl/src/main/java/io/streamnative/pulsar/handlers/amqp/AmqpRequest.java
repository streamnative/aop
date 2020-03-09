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
package io.streamnative.pulsar.handlers.amqp;

import static com.google.common.base.Preconditions.checkArgument;

import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.ContentBodyPayload;
import io.streamnative.pulsar.handlers.amqp.frame.ContentHeaderPayload;
import io.streamnative.pulsar.handlers.amqp.frame.HeartbeatPayload;
import io.streamnative.pulsar.handlers.amqp.frame.MethodPayload;
import io.streamnative.pulsar.handlers.amqp.frame.MissingFrameDelimiterException;
import io.streamnative.pulsar.handlers.amqp.frame.UnknownFrameException;
import io.streamnative.pulsar.handlers.amqp.frame.types.Octet;
import io.streamnative.pulsar.handlers.amqp.frame.types.Payload;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedInt;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;
import java.io.Closeable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * AmqpRequest will decode ByteBuf into a request.
 */
@Slf4j
@Getter
public class AmqpRequest implements Closeable {
    public static final UnsignedShort ADMIN_CHANNEL = new UnsignedShort(0);

    private static final Octet DELIMITER = new Octet(206);

    private final Octet frameId;
    private final UnsignedShort channel;
    private final UnsignedInt payloadSize;
    private final Payload payload;

    private final ByteBuf buffer;

    AmqpRequest(ByteBuf buffer) throws Exception {
        checkArgument(buffer.readableBytes() > 0);
        this.buffer = buffer.retain();

        this.frameId = new Octet(buffer);
        this.channel = new UnsignedShort(buffer);
        this.payloadSize = new UnsignedInt(buffer);

        if (log.isDebugEnabled()) {
            log.debug(String.format("Decoding frame '%s' on channel '%s'", frameId, channel));
        }

        this.payload = newPayload(frameId, payloadSize, buffer);

        if (!new Octet(buffer).equals(DELIMITER)) {
            throw new MissingFrameDelimiterException(buffer);
        }
    }

    ByteBuf getBuffer() {
        return this.buffer;
    }

    public String toString() {
        return String.format("AmqpRequest(channel=" + channel + " frameId=" + frameId
                             + " payloadSize=" + payloadSize + " payload=" + payload + ")");
    }

    protected static Payload newPayload(Octet frameId, UnsignedInt payloadSize, ByteBuf buffer) throws Exception {
        if (frameId.equals(MethodPayload.FRAME_ID)) {
            return new MethodPayload(buffer);
        } else if (frameId.equals(ContentHeaderPayload.FRAME_ID)) {
            return new ContentHeaderPayload(buffer);
        } else if (frameId.equals(ContentBodyPayload.FRAME_ID)) {
            return new ContentBodyPayload(buffer.readBytes((int) payloadSize.getUnsignedInt()));
        } else if (frameId.equals(HeartbeatPayload.FRAME_ID)) {
            return new HeartbeatPayload();
        } else {
            throw new UnknownFrameException(frameId);
        }
    }

    public AmqpResponse getErrorResponse() {
        // todo
        return null;
    }

    @Override
    public void close() {
        this.buffer.release();
    }
}
