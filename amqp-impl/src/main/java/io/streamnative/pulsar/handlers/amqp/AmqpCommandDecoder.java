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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;


/**
 * A decoder that decodes amqp requests and responses.
 */
@Slf4j
public abstract class AmqpCommandDecoder extends ChannelInboundHandlerAdapter {
    protected ChannelHandlerContext ctx;
    protected SocketAddress remoteAddress;
    private final PulsarService pulsarService;
    private final AmqpServiceConfiguration amqpConfig;
    @Getter
    protected AtomicBoolean isActive = new AtomicBoolean(false);
    protected AmqpBrokerDecoder brokerDecoder;

    protected AmqpCommandDecoder(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        this.pulsarService = pulsarService;
        this.amqpConfig = amqpConfig;
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Channel writability has changed to: {}", ctx.channel().isWritable());
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Get a buffer that contains the full frame
        ByteBuf buffer = (ByteBuf) msg;

        Channel nettyChannel = ctx.channel();
        checkState(nettyChannel.equals(this.ctx.channel()));

        try {
            brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
        } catch (Throwable e) {
            log.error("error while handle command:", e);
            close();
        } finally {
            // the amqpRequest has already held the reference.
            buffer.release();
        }
    }

    public AmqpBrokerDecoder getBrokerDecoder() {
        return brokerDecoder;
    }

    abstract void close();

    protected CompletableFuture<Optional<String>>
    getProtocolDataToAdvertise(InetSocketAddress pulsarAddress,
                               TopicName topic) {
        CompletableFuture<Optional<String>> returnFuture = new CompletableFuture<>();

        if (pulsarAddress == null) {
            log.error("[{}] failed get pulsar address, returned null.", topic.toString());

            returnFuture.complete(Optional.empty());
            return returnFuture;
        }

        if (log.isDebugEnabled()) {
            log.debug("Found broker for topic {} puslarAddress: {}",
                    topic, pulsarAddress);
        }

        // advertised data is write in  /loadbalance/brokers/advertisedAddress:webServicePort
        // here we get the broker url, need to find related webServiceUrl.
        ZooKeeperCache zkCache = pulsarService.getLocalZkCache();
        zkCache.getChildrenAsync(LoadManager.LOADBALANCE_BROKERS_ROOT, zkCache)
                .whenComplete((set, throwable) -> {
                    if (throwable != null) {
                        log.error("Error in getChildrenAsync(zk://loadbalance) for {}", pulsarAddress, throwable);
                        returnFuture.complete(Optional.empty());
                        return;
                    }

                    String hostAndPort = pulsarAddress.getHostName() + ":" + pulsarAddress.getPort();
                    List<String> matchBrokers = Lists.newArrayList();
                    // match host part of url
                    for (String activeBroker : set) {
                        if (activeBroker.startsWith(pulsarAddress.getHostName() + ":")) {
                            matchBrokers.add(activeBroker);
                        }
                    }

                    if (matchBrokers.isEmpty()) {
                        log.error("No node for broker {} under zk://loadbalance", pulsarAddress);
                        returnFuture.complete(Optional.empty());
                        return;
                    }

                    // Get a list of ServiceLookupData for each matchBroker.
                    List<CompletableFuture<Optional<ServiceLookupData>>> list = matchBrokers.stream()
                            .map(matchBroker ->
                                    zkCache.getDataAsync(
                                            String.format("%s/%s", LoadManager.LOADBALANCE_BROKERS_ROOT, matchBroker),
                                            (ZooKeeperCache.Deserializer<ServiceLookupData>)
                                                    pulsarService.getLoadManager().get().getLoadReportDeserializer()))
                            .collect(Collectors.toList());

                    FutureUtil.waitForAll(list)
                        .whenComplete((ignore, th) -> {
                            if (th != null) {
                                log.error("Error in getDataAsync() for {}", pulsarAddress, th);
                                returnFuture.complete(Optional.empty());
                                return;
                            }

                            try {
                                for (CompletableFuture<Optional<ServiceLookupData>> lookupData : list) {
                                    ServiceLookupData data = lookupData.get().get();
                                    if (log.isDebugEnabled()) {
                                        log.debug("Handle getProtocolDataToAdvertise for {}, pulsarUrl: {}, "
                                                        + "pulsarUrlTls: {}, webUrl: {}, webUrlTls: {} amqp: {}",
                                                topic, data.getPulsarServiceUrl(), data.getPulsarServiceUrlTls(),
                                                data.getWebServiceUrl(), data.getWebServiceUrlTls(),
                                                data.getProtocol(AmqpProtocolHandler.PROTOCOL_NAME));
                                    }

                                    if (lookupDataContainsAddress(data, hostAndPort)) {
                                        returnFuture.complete(data.getProtocol(AmqpProtocolHandler.PROTOCOL_NAME));
                                        return;
                                    }
                                }
                            } catch (Exception e) {
                                log.error("Error in {} lookupFuture get: ", pulsarAddress, e);
                                returnFuture.complete(Optional.empty());
                                return;
                            }

                            // no matching lookup data in all matchBrokers.
                            log.error("Not able to search {} in all child of zk://loadbalance", pulsarAddress);
                            returnFuture.complete(Optional.empty());
                        }
                        );
                });
        return returnFuture;
    }

    // whether a ServiceLookupData contains wanted address.
    static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return (data.getPulsarServiceUrl() != null && data.getPulsarServiceUrl().contains(hostAndPort))
                || (data.getPulsarServiceUrlTls() != null && data.getPulsarServiceUrlTls().contains(hostAndPort))
                || (data.getWebServiceUrl() != null && data.getWebServiceUrl().contains(hostAndPort))
                || (data.getWebServiceUrlTls() != null && data.getWebServiceUrlTls().contains(hostAndPort));
    }

    @VisibleForTesting
    public ChannelHandlerContext getCtx() {
        return ctx;
    }
}
