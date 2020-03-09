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

import com.google.common.collect.Lists;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.zookeeper.ZooKeeperCache;
import org.apache.pulsar.zookeeper.ZooKeeperCache.Deserializer;

/**
 * This class contains all the request handling methods.
 */
@Slf4j
@Getter
public class AmqpRequestHandler extends AmqpCommandDecoder {

    private final PulsarService pulsarService;
    private final AmqpServiceConfiguration amqpConfig;

    private final String clusterName;
    private final ScheduledExecutorService executor;
    private final PulsarAdmin admin;


    public AmqpRequestHandler(PulsarService pulsarService,
                               AmqpServiceConfiguration amqpConfig)
            throws Exception {
        super();
        this.pulsarService = pulsarService;
        this.amqpConfig = amqpConfig;
        this.clusterName = amqpConfig.getClusterName();
        this.executor = pulsarService.getExecutor();
        this.admin = pulsarService.getAdminClient();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        log.info("channel active: {}", ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        log.info("channel inactive {}", ctx.channel());

        close();
        isActive.set(false);
    }

    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Caught error in handler, closing channel", cause);
        ctx.close();
    }

    private CompletableFuture<Optional<String>>
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
                            (Deserializer<ServiceLookupData>)
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

    static AmqpResponse failedResponse(AmqpRequest amqpRequest, Throwable e) {
        if (log.isDebugEnabled()) {
            log.debug("Request {} get failed response ", amqpRequest, e);
        }
        return amqpRequest.getErrorResponse();
    }

    // whether a ServiceLookupData contains wanted address.
    static boolean lookupDataContainsAddress(ServiceLookupData data, String hostAndPort) {
        return (data.getPulsarServiceUrl() != null && data.getPulsarServiceUrl().contains(hostAndPort))
            || (data.getPulsarServiceUrlTls() != null && data.getPulsarServiceUrlTls().contains(hostAndPort))
            || (data.getWebServiceUrl() != null && data.getWebServiceUrl().contains(hostAndPort))
            || (data.getWebServiceUrlTls() != null && data.getWebServiceUrlTls().contains(hostAndPort));
    }


    protected void handleMethodFrame(AmqpRequest request,
                                     CompletableFuture<AmqpResponse> responseFuture) {
        // todo
    }

    protected void handleContentHeaderFrame(AmqpRequest request,
                                            CompletableFuture<AmqpResponse> responseFuture) {
        // todo
    }

    protected void handleContentBodyFrame(AmqpRequest request,
                                          CompletableFuture<AmqpResponse> responseFuture) {
        // todo
    }

    protected void handleHeartbeaatFrame(AmqpRequest request,
                                         CompletableFuture<AmqpResponse> responseFuture) {
        // todo
    }

    protected void handleError(AmqpRequest request,
                               CompletableFuture<AmqpResponse> responseFuture) {
        // todo
    }
}
