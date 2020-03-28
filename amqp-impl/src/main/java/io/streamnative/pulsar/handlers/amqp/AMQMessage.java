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
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;
import org.apache.qpid.server.protocol.v0_8.transport.MessagePublishInfo;

/**
 * Amqp message model.
 */
public class AMQMessage {

    private final MessagePublishInfo messagePublishInfo;
    private ContentHeaderBody contentHeaderBody;

    /**
     * Keeps a track of how many bytes we have received in body frames.
     */
    private long bodyLengthReceived = 0;
    private ContentBody contentBody;

    public AMQMessage(MessagePublishInfo messagePublishInfo, ContentHeaderBody contentHeaderBody,
        ContentBody contentBody) {
        this.messagePublishInfo = messagePublishInfo;
        this.contentHeaderBody = contentHeaderBody;
        this.contentBody = contentBody;
    }

    public AMQMessage(MessagePublishInfo info) {
        messagePublishInfo = info;
    }

    public void setContentHeaderBody(final ContentHeaderBody contentHeaderBody) {
        this.contentHeaderBody = contentHeaderBody;
    }

    public MessagePublishInfo getMessagePublishInfo() {
        return messagePublishInfo;
    }

    public boolean allContentReceived() {
        return (bodyLengthReceived == getContentHeader().getBodySize());
    }

    public AMQShortString getExchangeName() {
        return messagePublishInfo.getExchange();
    }

    public ContentHeaderBody getContentHeader() {
        return contentHeaderBody;
    }

    public long getSize() {
        return getContentHeader().getBodySize();
    }

    public ContentBody getContentBody() {
        return contentBody;
    }

    public void setContentBody(ContentBody contentBody) {
        this.contentBody = contentBody;
    }
}
