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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.RedeliveryTracker;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Dispatcher mock test.
 */
public class MockDispatcher implements Dispatcher {
    @Override public void addConsumer(Consumer consumer) throws BrokerServiceException {

    }

    @Override public void removeConsumer(Consumer consumer) throws BrokerServiceException {

    }

    @Override public void consumerFlow(Consumer consumer, int additionalNumberOfMessages) {

    }

    @Override public boolean isConsumerConnected() {
        return false;
    }

    @Override public List<Consumer> getConsumers() {
        return null;
    }

    @Override public boolean canUnsubscribe(Consumer consumer) {
        return false;
    }

    @Override public CompletableFuture<Void> close() {
        return null;
    }

    @Override public boolean isClosed() {
        return false;
    }

    @Override
    public CompletableFuture<Void> disconnectActiveConsumers(boolean b) {
        return null;
    }

    @Override public CompletableFuture<Void> disconnectAllConsumers(boolean isResetCursor) {
        return null;
    }

    @Override
    public CompletableFuture<Void> disconnectAllConsumers() {
        return null;
    }

    @Override public void resetCloseFuture() {

    }

    @Override public void reset() {

    }

    @Override public CommandSubscribe.SubType getType() {
        return null;
    }

    @Override public void redeliverUnacknowledgedMessages(Consumer consumer, long consumerEpoch) {
        // nothing to do
    }

    @Override public void redeliverUnacknowledgedMessages(Consumer consumer, List<PositionImpl> positions) {

    }

    @Override public void addUnAckedMessages(int unAckMessages) {

    }

    @Override public RedeliveryTracker getRedeliveryTracker() {
        return null;
    }

    @Override
    public Optional<DispatchRateLimiter> getRateLimiter() {
        return Optional.empty();
    }

    @Override
    public boolean initializeDispatchRateLimiterIfNeeded() {
        return false;
    }

    @Override
    public boolean trackDelayedDelivery(long ledgerId, long entryId, MessageMetadata msgMetadata) {
        return false;
    }

    @Override
    public long getNumberOfDelayedMessages() {
        return 0;
    }

    @Override
    public void cursorIsReset() {

    }
}
