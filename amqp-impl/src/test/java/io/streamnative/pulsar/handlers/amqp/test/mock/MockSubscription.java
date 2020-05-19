
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

package io.streamnative.pulsar.handlers.amqp.test.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.api.proto.PulsarApi;

/**
 * Subscription mock test.
 */
public class MockSubscription implements Subscription {



    @Override public Topic getTopic() {
        return null;
    }

    @Override public String getName() {
        return null;
    }

    @Override public void addConsumer(Consumer consumer) throws BrokerServiceException {

    }

    @Override public void removeConsumer(Consumer consumer, boolean isResetCursor) throws BrokerServiceException {

    }

    @Override public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

    }

    @Override public void acknowledgeMessage(List<Position> positions, PulsarApi.CommandAck.AckType ackType,
        Map<String, Long> properties) {

    }

    @Override public String getTopicName() {
        return null;
    }

    @Override public boolean isReplicated() {
        return false;
    }

    @Override public Dispatcher getDispatcher() {
        return null;
    }

    @Override
    public long getNumberOfEntriesInBacklog(boolean b) {
        return 0;
    }

    @Override public List<Consumer> getConsumers() {
        return null;
    }

    @Override public CompletableFuture<Void> close() {
        return null;
    }

    @Override public CompletableFuture<Void> delete() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return null;
    }

    @Override public CompletableFuture<Void> disconnect() {
        return null;
    }

    @Override public CompletableFuture<Void> doUnsubscribe(Consumer consumer) {
        return null;
    }

    @Override public CompletableFuture<Void> clearBacklog() {
        return null;
    }

    @Override public CompletableFuture<Void> skipMessages(int numMessagesToSkip) {
        return null;
    }

    @Override public CompletableFuture<Void> resetCursor(long timestamp) {
        return null;
    }

    @Override public CompletableFuture<Void> resetCursor(Position position) {
        return null;
    }

    @Override public CompletableFuture<Entry> peekNthMessage(int messagePosition) {
        return null;
    }

    @Override public void expireMessages(int messageTTLInSeconds) {

    }

    @Override public void redeliverUnacknowledgedMessages(Consumer consumer) {

    }

    @Override public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {

    }

    @Override public void markTopicWithBatchMessagePublished() {

    }

    @Override public double getExpiredMessageRate() {
        return 0;
    }

    @Override public PulsarApi.CommandSubscribe.SubType getType() {
        return null;
    }

    @Override public String getTypeString() {
        return null;
    }

    @Override public void addUnAckedMessages(int unAckMessages) {

    }
}
