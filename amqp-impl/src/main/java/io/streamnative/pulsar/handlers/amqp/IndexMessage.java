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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Recycler;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * Index message used by index message persistent.
 * The queue can read the real message data from the exchange through the index message.
 */
@Slf4j
public class IndexMessage {

    private static final String X_DELAY = "_bph_.x-delay";

    /**
     * Name of the exchange that the message data from.
     */
    private String exchangeName;

    /**
     * Ledger ID of the message data that stored in the exchange.
     */
    private long ledgerId;

    /**
     * Entry ID of the message data that stored in the exchange.
     */
    private long entryId;

    /**
     * Properties of the message.
     */
    private Map<String, Object> properties;

    public static IndexMessage create(String exchangeName, long ledgerId, long entryId,
                                      Map<String, Object> properties) {
        IndexMessage instance = RECYCLER.get();
        instance.exchangeName = exchangeName;
        instance.ledgerId = ledgerId;
        instance.entryId = entryId;
        instance.properties = properties;
        return instance;
    }

    public static IndexMessage create(byte[] bytes) {
        IndexMessage instance = RECYCLER.get();
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        instance.ledgerId = byteBuf.readLong();
        instance.entryId = byteBuf.readLong();
        instance.exchangeName = byteBuf.readCharSequence(byteBuf.readableBytes(), StandardCharsets.UTF_8).toString();
        return instance;
    }

    private Recycler.Handle<IndexMessage> recyclerHandle;

    private static final Recycler<IndexMessage> RECYCLER = new Recycler<IndexMessage>() {
        @Override
        protected IndexMessage newObject(Recycler.Handle<IndexMessage> recyclerHandle) {
            return new IndexMessage(recyclerHandle);
        }
    };

    private IndexMessage(Recycler.Handle<IndexMessage> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public void recycle() {
        recyclerHandle.recycle(this);
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public byte[] encode() {
        int size = 16 + exchangeName.getBytes(StandardCharsets.ISO_8859_1).length;
        ByteBuf byteBuf = Unpooled.buffer(size);
        byteBuf.writeLong(ledgerId);
        byteBuf.writeLong(entryId);
        byteBuf.writeCharSequence(exchangeName, StandardCharsets.ISO_8859_1);
        return byteBuf.array();
    }

    public long getDeliverAtTime(){
        Object delayValue;
        long delayTime = 0;
        if (properties != null && (delayValue = properties.get(X_DELAY)) != null) {
            try {
                delayTime = Long.parseLong(delayValue.toString());
            } catch (NumberFormatException e) {
                log.warn("Failed to parse the number according to x-delay. x-delay value:{}", delayValue, e);
            }
        }
        return delayTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IndexMessage that = (IndexMessage) o;
        return ledgerId == that.ledgerId && entryId == that.entryId && exchangeName.equals(that.exchangeName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(exchangeName, ledgerId, entryId);
    }
}
