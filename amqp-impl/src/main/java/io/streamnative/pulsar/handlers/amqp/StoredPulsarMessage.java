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

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoredMessage;


// no use
public class StoredPulsarMessage<T extends StorableMessageMetaData> implements StoredMessage<T>, MessageHandle<T> {

    private T _metaData;

    private QpidByteBuffer _content = null;

    public StoredPulsarMessage(T _metaData) {
        this._metaData = _metaData;
    }

    @Override
    public void addContent(QpidByteBuffer src) {
        try (QpidByteBuffer content = _content) {
            if (content == null) {
                _content = src.slice();
            } else {
                _content = QpidByteBuffer.concatenate(content, src);
            }
        }
    }

    @Override
    public StoredMessage<T> allContentAdded() {
        return null;
    }

    @Override
    public T getMetaData() {
        return null;
    }

    @Override
    public long getMessageNumber() {
        return 0;
    }

    @Override
    public QpidByteBuffer getContent(int i, int i1) {
        return null;
    }

    @Override
    public int getContentSize() {
        return 0;
    }

    @Override
    public int getMetadataSize() {
        return 0;
    }

    @Override
    public void remove() {

    }

    @Override
    public boolean isInContentInMemory() {
        return false;
    }

    @Override
    public long getInMemorySize() {
        return 0;
    }

    @Override
    public boolean flowToDisk() {
        return false;
    }

    @Override
    public void reallocate() {

    }

}
