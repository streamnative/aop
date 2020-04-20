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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Producer;
import org.apache.pulsar.broker.service.Replicator;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.stats.ClusterReplicationMetrics;
import org.apache.pulsar.broker.stats.NamespaceStats;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.protocol.schema.SchemaData;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.utils.StatsOutputStream;


/**
 * Mock topic.
 */
public class MockTopic implements Topic {

    ConcurrentOpenHashMap<String, ? extends Subscription> subscriptions = new ConcurrentOpenHashMap<>();

    @Override
    public void publishMessage(ByteBuf byteBuf, PublishContext publishContext) {

    }

    @Override
    public void addProducer(Producer producer) throws BrokerServiceException {

    }

    @Override
    public void removeProducer(Producer producer) {

    }

    @Override
    public void recordAddLatency(long l, TimeUnit timeUnit) {

    }

    @Override
    public CompletableFuture<Consumer> subscribe(ServerCnx serverCnx, String s, long l,
                                                 PulsarApi.CommandSubscribe.SubType subType,
                                                 int i, String s1, boolean b,
                                                 MessageId messageId, Map<String, String> map,
                                                 boolean b1,
                                                 PulsarApi.CommandSubscribe.InitialPosition initialPosition,
                                                 long l1, boolean b2, PulsarApi.KeySharedMeta keySharedMeta) {
        return null;
    }

    @Override
    public CompletableFuture<Subscription> createSubscription(String s,
                                            PulsarApi.CommandSubscribe.InitialPosition initialPosition, boolean b) {
        CompletableFuture<Subscription> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public CompletableFuture<Void> unsubscribe(String s) {
        return null;
    }

    @Override
    public ConcurrentOpenHashMap<String, ? extends Subscription> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public CompletableFuture<Void> delete() {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        completableFuture.complete(null);
        return completableFuture;
    }

    @Override
    public Map<String, Producer> getProducers() {
        return null;
    }

    @Override
    public String getName() {
        return "persistent://public/topic";
    }

    @Override
    public CompletableFuture<Void> checkReplication() {
        return null;
    }

    @Override
    public CompletableFuture<Void> close(boolean b) {
        return null;
    }

    @Override
    public void checkGC(int i, InactiveTopicDeleteMode inactiveTopicDeleteMode) {

    }


    @Override
    public void checkInactiveSubscriptions() {

    }

    @Override
    public void checkMessageExpiry() {

    }

    @Override
    public void checkMessageDeduplicationInfo() {

    }

    @Override
    public void checkTopicPublishThrottlingRate() {

    }

    @Override
    public void incrementPublishCount(int i, long l) {

    }

    @Override
    public void resetTopicPublishCountAndEnableReadIfRequired() {

    }

    @Override
    public void resetBrokerPublishCountAndEnableReadIfRequired(boolean b) {

    }

    @Override
    public boolean isPublishRateExceeded() {
        return false;
    }

    @Override
    public CompletableFuture<Void> onPoliciesUpdate(Policies policies) {
        return null;
    }

    @Override
    public boolean isBacklogQuotaExceeded(String s) {
        return false;
    }

    @Override
    public boolean isEncryptionRequired() {
        return false;
    }

    @Override
    public boolean getSchemaValidationEnforced() {
        return false;
    }

    @Override
    public boolean isReplicated() {
        return false;
    }

    @Override
    public BacklogQuota getBacklogQuota() {
        return null;
    }

    @Override
    public void updateRates(NamespaceStats namespaceStats,
                            NamespaceBundleStats namespaceBundleStats,
                            StatsOutputStream statsOutputStream,
                            ClusterReplicationMetrics clusterReplicationMetrics,
                            String s, boolean b) {

    }

    @Override
    public Subscription getSubscription(String s) {
        Subscription subscription = subscriptions.get(s);
        if (subscription == null) {
            subscription = new MockSubscription();
        }
        return subscription;
    }

    @Override
    public ConcurrentOpenHashMap<String, ? extends Replicator> getReplicators() {
        return null;
    }

    @Override
    public TopicStats getStats(boolean b) {
        return null;
    }

    @Override
    public PersistentTopicInternalStats getInternalStats() {
        return null;
    }

    @Override
    public Position getLastPosition() {
        return null;
    }

    @Override
    public CompletableFuture<MessageId> getLastMessageId() {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> hasSchema() {
        return null;
    }

    @Override
    public CompletableFuture<SchemaVersion> addSchema(SchemaData schemaData) {
        return null;
    }

    @Override
    public CompletableFuture<SchemaVersion> deleteSchema() {
        return null;
    }

    @Override
    public CompletableFuture<Void> checkSchemaCompatibleForConsumer(SchemaData schemaData) {
        return null;
    }

    @Override
    public CompletableFuture<Void> addSchemaIfIdleOrCheckCompatible(SchemaData schemaData) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteForcefully() {
        return null;
    }
}
