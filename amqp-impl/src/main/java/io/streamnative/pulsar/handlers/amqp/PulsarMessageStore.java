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

import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.store.Event;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.MessageHandle;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.StorableMessageMetaData;
import org.apache.qpid.server.store.StoreException;
import org.apache.qpid.server.store.Transaction;

import java.io.File;


// no use
public class PulsarMessageStore implements MessageStore {
    public static final String TYPE = "Pulsar";

    @Override
    public long getNextMessageId() {
        return 0;
    }

    @Override
    public String getStoreLocation() {
        return null;
    }

    @Override
    public File getStoreLocationAsFile() {
        return null;
    }

    @Override
    public void addEventListener(EventListener eventListener, Event... events) {

    }

    @Override
    public void openMessageStore(ConfiguredObject<?> configuredObject) {

    }

    @Override
    public void upgradeStoreStructure() throws StoreException {

    }

    @Override
    public <T extends StorableMessageMetaData> MessageHandle<T> addMessage(T metaData) {
        StoredPulsarMessage<T> storedPulsarMessage = new StoredPulsarMessage<>(metaData);
        return storedPulsarMessage;
    }

    @Override
    public long getInMemorySize() {
        return 0;
    }

    @Override
    public long getBytesEvacuatedFromMemory() {
        return 0;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public Transaction newTransaction() {
        return null;
    }

    @Override
    public void closeMessageStore() {

    }

    @Override
    public void onDelete(ConfiguredObject<?> configuredObject) {

    }

    @Override
    public void addMessageDeleteListener(MessageDeleteListener messageDeleteListener) {

    }

    @Override
    public void removeMessageDeleteListener(MessageDeleteListener messageDeleteListener) {

    }

    @Override
    public MessageStoreReader newMessageStoreReader() {
        return null;
    }

}
