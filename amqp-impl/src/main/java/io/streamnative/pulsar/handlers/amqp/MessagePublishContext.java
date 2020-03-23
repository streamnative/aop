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

import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.messageToByteBuf;
import static io.streamnative.pulsar.handlers.amqp.utils.MessageConvertUtils.toPulsarMessage;

import io.netty.buffer.ByteBuf;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.Topic.PublishContext;
import org.apache.qpid.server.protocol.v0_8.IncomingMessage;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;


/**
 * Implementation for PublishContext.
 */
@Slf4j
public final class MessagePublishContext implements PublishContext {

    private Topic topic;
    private long startTimeNs;
    public static final boolean MESSAGE_BATCHED = false;


    /**
     * Executed from managed ledger thread when the message is persisted.
     */
    @Override
    public void completed(Exception exception, long ledgerId, long entryId) {
        recycle();
    }

    // recycler
    public static MessagePublishContext get(Topic topic,
                                            long startTimeNs) {
        MessagePublishContext callback = RECYCLER.get();
        callback.topic = topic;
        callback.startTimeNs = startTimeNs;
        return callback;
    }

    private final Handle<MessagePublishContext> recyclerHandle;

    private MessagePublishContext(Handle<MessagePublishContext> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    private static final Recycler<MessagePublishContext> RECYCLER = new Recycler<MessagePublishContext>() {
        protected MessagePublishContext newObject(Handle<MessagePublishContext> handle) {
            return new MessagePublishContext(handle);
        }
    };

    public void recycle() {
        topic = null;
        startTimeNs = -1;
        recyclerHandle.recycle(this);
    }

    /**
     * publish amqp message to pulsar topic, no batch.
     */
    public static void publishMessages(IncomingMessage incomingMessage, Topic topic) {
        ByteBuf headerAndPayload = messageToByteBuf(toPulsarMessage(incomingMessage));
        topic.publishMessage(headerAndPayload,
                MessagePublishContext.get(topic, System.nanoTime()));
    }
}
