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

import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats;

/**
 * ManagedLedger mock test.
 */
public class MockManagedLedger implements ManagedLedger {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public Position addEntry(byte[] bytes) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] bytes, AsyncCallbacks.AddEntryCallback addEntryCallback, Object o) {

    }

    @Override
    public Position addEntry(byte[] bytes, int i, int i1) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] bytes, int i, int i1, AsyncCallbacks.AddEntryCallback addEntryCallback, Object o) {

    }

    @Override
    public void asyncAddEntry(ByteBuf byteBuf, AsyncCallbacks.AddEntryCallback addEntryCallback, Object o) {

    }

    @Override
    public ManagedCursor openCursor(String s) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor openCursor(String s, CommandSubscribe.InitialPosition initialPosition)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position position) throws ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position position, String s) throws ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position startPosition, String subscriptionName,
                                             CommandSubscribe.InitialPosition initialPosition, boolean isReadCompacted)
            throws ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncDeleteCursor(String s, AsyncCallbacks.DeleteCursorCallback deleteCursorCallback, Object o) {

    }

    @Override
    public void deleteCursor(String s) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncOpenCursor(String s, AsyncCallbacks.OpenCursorCallback openCursorCallback, Object o) {

    }

    @Override
    public void asyncOpenCursor(String s, CommandSubscribe.InitialPosition initialPosition,
                                AsyncCallbacks.OpenCursorCallback openCursorCallback, Object o) {

    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return null;
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return null;
    }

    @Override
    public long getNumberOfEntries() {
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        return 0;
    }

    @Override
    public long getTotalSize() {
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        return 0;
    }

    @Override
    public long getOffloadedSize() {
        return 0;
    }

    @Override
    public void asyncTerminate(AsyncCallbacks.TerminateCallback terminateCallback, Object o) {

    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback closeCallback, Object o) {

    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return null;
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(AsyncCallbacks.DeleteLedgerCallback deleteLedgerCallback, Object o) {

    }

    @Override
    public Position offloadPrefix(Position position) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncOffloadPrefix(Position position, AsyncCallbacks.OffloadCallback offloadCallback, Object o) {

    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return null;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return null;
    }

    @Override
    public void setConfig(ManagedLedgerConfig managedLedgerConfig) {

    }

    @Override
    public Position getLastConfirmedEntry() {
        return null;
    }

    @Override
    public void readyToCreateNewLedger() {

    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public void setProperty(String s, String s1) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncSetProperty(String s, String s1, AsyncCallbacks.UpdatePropertiesCallback updatePropertiesCallback,
                                 Object o) {

    }

    @Override
    public void deleteProperty(String s) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDeleteProperty(String s, AsyncCallbacks.UpdatePropertiesCallback updatePropertiesCallback,
                                    Object o) {

    }

    @Override
    public void setProperties(Map<String, String> map) throws InterruptedException {

    }

    @Override
    public void asyncSetProperties(Map<String, String> map,
                                   AsyncCallbacks.UpdatePropertiesCallback updatePropertiesCallback, Object o) {

    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> completableFuture) {

    }

    @Override
    public void rollCurrentLedgerIfFull() {

    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        // nothing
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages,
                              AsyncCallbacks.AddEntryCallback callback, Object ctx) {
        // nothing
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return null;
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return null;
    }

    @Override
    public CompletableFuture<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgerInfo(long ledgerId) {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncTruncate() {
        return null;
    }

    @Override
    public CompletableFuture<ManagedLedgerInternalStats> getManagedLedgerInternalStats(boolean includeLedgerMetadata) {
        return null;
    }

    @Override
    public long getLastOffloadedLedgerId() {
        return 0;
    }

    @Override
    public long getLastOffloadedSuccessTimestamp() {
        return 0;
    }

    @Override
    public long getLastOffloadedFailureTimestamp() {
        return 0;
    }

    @Override
    public CompletableFuture<Long> getEarliestMessagePublishTimeInBacklog() {
        return null;
    }

    @Override
    public void checkInactiveLedgerAndRollOver() {
        // nothing to do
    }

    @Override
    public void removeWaitingCursor(ManagedCursor cursor) {
        // nothing to do
    }

    @Override
    public ManagedCursor openCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                    Map<String, Long> properties, Map<String, String> cursorProperties)
            throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncOpenCursor(String name, CommandSubscribe.InitialPosition initialPosition,
                                Map<String, Long> properties, Map<String, String> cursorProperties,
                                AsyncCallbacks.OpenCursorCallback callback, Object ctx) {
        // nothing to do
    }

}
