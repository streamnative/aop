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
package io.streamnative.pulsar.handlers.amqp.proxy;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.streamnative.pulsar.handlers.amqp.AmqpBrokerDecoder;
import io.streamnative.pulsar.handlers.amqp.AmqpConnection;
import io.streamnative.pulsar.handlers.amqp.AmqpProtocolHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionTuneBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

/**
 * Proxy connection.
 */
@Slf4j
public class ProxyConnection extends ChannelInboundHandlerAdapter implements
        ServerMethodProcessor<ServerChannelMethodProcessor>, FutureListener<Void> {

    private ProxyService proxyService;
    private ProxyConfiguration proxyConfig;
    @Getter
    private ChannelHandlerContext cnx;
    private State state;
    private ProxyHandler proxyHandler;

    protected AmqpBrokerDecoder brokerDecoder;
    @Getter
    private MethodRegistry methodRegistry;
    private ProtocolVersion protocolVersion;
    private LookupHandler lookupHandler;
    private AMQShortString virtualHost;
    private String vhost;

    private List<Object> connectMsgList = new ArrayList<>();

    private enum State {
        Init,
        RedirectLookup,
        RedirectToBroker,
        Closed
    }

    public ProxyConnection(ProxyService proxyService) throws PulsarClientException {
        log.info("ProxyConnection init ...");
        this.proxyService = proxyService;
        this.proxyConfig = proxyService.getProxyConfig();
        brokerDecoder = new AmqpBrokerDecoder(this);
        protocolVersion = ProtocolVersion.v0_91;
        methodRegistry = new MethodRegistry(protocolVersion);
        lookupHandler = proxyService.getLookupHandler();
        state = State.Init;
    }

    @Override
    public void channelActive(ChannelHandlerContext cnx) throws Exception {
        super.channelActive(cnx);
        this.cnx = cnx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
        this.close();
    }

    @Override
    public void operationComplete(Future future) throws Exception {
        // This is invoked when the write operation on the paired connection is
        // completed
        if (future.isSuccess()) {
            cnx.read();
        } else {
            log.warn("Error in writing to inbound channel. Closing", future.cause());
            proxyHandler.getBrokerChannel().close();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        log.info("ProxyConnection [channelRead] - access msg: {}", ((ByteBuf) msg));
        switch (state) {
            case Init:
            case RedirectLookup:
                log.info("ProxyConnection [channelRead] - RedirectLookup");
                connectMsgList.add(msg);

                // Get a buffer that contains the full frame
                ByteBuf buffer = (ByteBuf) msg;

                io.netty.channel.Channel nettyChannel = ctx.channel();
                checkState(nettyChannel.equals(this.cnx.channel()));

                try {
                    brokerDecoder.decodeBuffer(QpidByteBuffer.wrap(buffer.nioBuffer()));
                } catch (Throwable e) {
                    log.error("error while handle command:", e);
                    close();
                }

                break;
            case RedirectToBroker:
                if (log.isDebugEnabled()) {
                    log.debug("ProxyConnection [channelRead] - RedirectToBroker");
                }
                if (proxyHandler != null) {
                    proxyHandler.getBrokerChannel().writeAndFlush(msg);
                }
                break;
            case Closed:
                log.info("ProxyConnection [channelRead] - closed");
                break;
            default:
                log.info("ProxyConnection [channelRead] - invalid state");
                break;
        }
    }

    // step 1
    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveProtocolHeader] Protocol Header [{}]", protocolInitiation);
        }
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = protocolInitiation.checkVersion(); // Fails if not correct
            // TODO serverProperties mechanis
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                    (short) protocolVersion.getMajorVersion(),
                    (short) pv.getActualMinorVersion(),
                    null,
                    // TODO temporary modification
                    "PLAIN".getBytes(US_ASCII),
                    "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
        } catch (QpidException e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
        }
    }

    // step 2
    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
                                         AMQShortString locale) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionStartOk] clientProperties: {}, mechanism: {}, locale: {}",
                    clientProperties, mechanism, locale);
        }
        // TODO AUTH
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(proxyConfig.getAmqpMaxNoOfChannels(),
                        proxyConfig.getAmqpMaxFrameSize(), proxyConfig.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    // step 3
    @Override
    public void receiveConnectionSecureOk(byte[] response) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionSecureOk] response: {}", new String(response, UTF_8));
        }
        ConnectionTuneBody tuneBody =
                methodRegistry.createConnectionTuneBody(proxyConfig.getAmqpMaxNoOfChannels(),
                        proxyConfig.getAmqpMaxFrameSize(), proxyConfig.getAmqpHeartBeat());
        writeFrame(tuneBody.generateFrame(0));
    }

    // step 4
    @Override
    public void receiveConnectionTuneOk(int i, long l, int i1) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionTuneOk]");
        }
    }

    // step 5
    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionOpen] virtualHost: {} capabilities: {} insist: {}",
                    virtualHost, capabilities, insist);
        }
        this.virtualHost = virtualHost;
        state = State.RedirectLookup;
        String virtualHostStr = AMQShortString.toString(virtualHost);
        if ((virtualHostStr != null) && virtualHostStr.charAt(0) == '/') {
            virtualHostStr = virtualHostStr.substring(1);
            if (org.apache.commons.lang.StringUtils.isEmpty(virtualHostStr)){
                virtualHostStr = AmqpConnection.DEFAULT_NAMESPACE;
            }
        }
        vhost = virtualHostStr;

        proxyService.getVhostConnectionMap().compute(vhost, (v, set) -> {
            if (set == null) {
                Set<ProxyConnection> proxyConnectionSet =  Sets.newConcurrentHashSet();
                proxyConnectionSet.add(this);
                return proxyConnectionSet;
            } else {
                set.add(this);
                return set;
            }
        });
        handleConnect(new AtomicInteger(5));
    }

    public void handleConnect(AtomicInteger retryTimes) {
        log.info("handle connect residue retryTimes: {}", retryTimes);
        if (retryTimes.get() == 0) {
            log.warn("Handle connect retryTime is 0.");
            return;
        }
        if (proxyService.getVhostBrokerMap().containsKey(vhost)) {
            String aopBrokerHost = proxyService.getVhostBrokerMap().get(vhost).getLeft();
            int aopBrokerPort = proxyService.getVhostBrokerMap().get(vhost).getRight();
            handleConnectComplete(aopBrokerHost, aopBrokerPort, retryTimes);
        } else {
            try {
                NamespaceName namespaceName = NamespaceName.get(proxyConfig.getAmqpTenant(), vhost);

                String topic = TopicName.get(TopicDomain.persistent.value(),
                        namespaceName, "__lookup__").toString();
                CompletableFuture<Pair<String, Integer>> lookupData = lookupHandler.findBroker(
                        TopicName.get(topic), AmqpProtocolHandler.PROTOCOL_NAME);
                lookupData.whenComplete((pair, throwable) -> {
                    handleConnectComplete(pair.getLeft(), pair.getRight(), retryTimes);
                    proxyService.cacheVhostMap(vhost, pair);
                });
            } catch (Exception e) {
                log.error("Lookup broker failed.", e);
                resetProxyHandler();
            }
        }
    }

    private void handleConnectComplete(String aopBrokerHost, int aopBrokerPort, AtomicInteger retryTimes) {
        try {
            if (StringUtils.isEmpty(aopBrokerHost) || aopBrokerPort == 0) {
                throw new ProxyException();
            }

            AMQMethodBody responseBody = methodRegistry.createConnectionOpenOkBody(virtualHost);
            proxyHandler = new ProxyHandler(vhost, proxyService,
                    this, aopBrokerHost, aopBrokerPort, connectMsgList, responseBody);
            state = State.RedirectToBroker;
            log.info("Handle connect complete. aopBrokerHost: {}, aopBrokerPort: {}", aopBrokerHost, aopBrokerPort);
        } catch (Exception e) {
            retryTimes.decrementAndGet();
            resetProxyHandler();
            String errorMSg = String.format("Lookup broker failed. aopBrokerHost: %S, aopBrokerPort: %S",
                    aopBrokerHost, aopBrokerPort);
            log.error(errorMSg, e);
            handleConnect(retryTimes);
        }
    }

    public void resetProxyHandler() {
        if (proxyHandler != null) {
            proxyHandler.close();
            proxyHandler = null;
        }
    }

    @Override
    public void receiveChannelOpen(int i) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveChannelOpen]");
        }
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [getProtocolVersion]");
        }
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int i) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [getChannelMethodProcessor]");
        }
        return null;
    }

    @Override
    public void receiveConnectionClose(int i, AMQShortString amqShortString, int i1, int i2) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionClose]");
        }
    }

    @Override
    public void receiveConnectionCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveConnectionCloseOk]");
        }
    }

    @Override
    public void receiveHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [receiveHeartbeat]");
        }
    }


    @Override
    public void setCurrentMethod(int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [setCurrentMethod] classId: {}, methodId: {}", classId, methodId);
        }
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection - [ignoreAllButCloseOk]");
        }
        return false;
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("send: " + frame);
        }
        cnx.writeAndFlush(frame);
    }

    public void close() {
        log.info("ProxyConnection close.");
        if (log.isDebugEnabled()) {
            log.debug("ProxyConnection close.");
        }

        if (proxyHandler != null) {
            resetProxyHandler();
        }
        if (cnx != null) {
            cnx.close();
        }
        state = State.Closed;
    }

}
