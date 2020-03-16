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

import static java.nio.charset.StandardCharsets.US_ASCII;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.ChannelHandlerContext;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.log4j.Log4j2;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.qpid.server.QpidException;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.AMQFrame;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ConnectionCloseBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;
import org.apache.qpid.server.transport.ByteBufferSender;

/**
 * Amqp server level method processor.
 */
@Log4j2
public class AmqpConnection extends AmqpCommandDecoder implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    enum ConnectionState {
        INIT,
        AWAIT_START_OK,
        AWAIT_SECURE_OK,
        AWAIT_TUNE_OK,
        AWAIT_OPEN,
        OPEN
    }

    private final ConcurrentLongHashMap<AmqpChannel> channels;
    private ProtocolVersion protocolVersion;
    private MethodRegistry methodRegistry;
    private ByteBufferSender bufferSender;
    private volatile ConnectionState state = ConnectionState.INIT;
    private volatile int currentClassId;
    private volatile int currentMethodId;
    private final AtomicBoolean orderlyClose = new AtomicBoolean(false);

    public AmqpConnection(PulsarService pulsarService, AmqpServiceConfiguration amqpConfig) {
        super(pulsarService, amqpConfig);
        this.channels = new ConcurrentLongHashMap<>();
        this.protocolVersion = ProtocolVersion.v0_91;
        this.methodRegistry = new MethodRegistry(this.protocolVersion);
        this.bufferSender = new AmqpByteBufferSender(this);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.remoteAddress = ctx.channel().remoteAddress();
        this.ctx = ctx;
        isActive.set(true);
        this.brokerDecoder = new AmqpBrokerDecoder(this);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("[{}] Got exception: {}", remoteAddress, cause.getMessage(), cause);
        close();
    }

    @Override
    protected void close() {
        if (isActive.getAndSet(false)) {
            log.info("close netty channel {}", ctx.channel());
            ctx.close();
        }
    }

    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
             AMQShortString locale) {
        if (log.isDebugEnabled()) {
            log.debug("RECV ConnectionStartOk[clientProperties: {}, mechanism: {}, locale: {}]",
                    clientProperties, mechanism, locale);
        }
        assertState(ConnectionState.AWAIT_START_OK);
        AMQMethodBody responseBody = this.methodRegistry.createConnectionSecureBody(new byte[0]);
        writeFrame(responseBody.generateFrame(0));
        state = ConnectionState.AWAIT_SECURE_OK;
        bufferSender.flush();
    }

    @Override
    public void receiveConnectionSecureOk(byte[] response) {

    }

    @Override
    public void receiveConnectionTuneOk(int channelMax, long frameMax, int heartbeat) {

    }

    @Override
    public void receiveConnectionOpen(AMQShortString virtualHost, AMQShortString capabilities, boolean insist) {

    }

    @Override
    public void receiveChannelOpen(int channelId) {
        this.channels.put(channelId, new AmqpChannel(this, channelId));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        return this.channels.get(channelId);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {

    }

    @Override
    public void receiveConnectionCloseOk() {

    }

    @Override
    public void receiveHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("RECV Heartbeat");
        }
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation pi) {
        if (log.isDebugEnabled()) {
            log.debug("RECV Protocol Header [{}]", pi);
        }
        brokerDecoder.setExpectProtocolInitiation(false);
        try {
            ProtocolVersion pv = pi.checkVersion(); // Fails if not correct
            AMQMethodBody responseBody = this.methodRegistry.createConnectionStartBody(
                    (short) protocolVersion.getMajorVersion(),
                    (short) pv.getActualMinorVersion(),
                    null,
                    "".getBytes(US_ASCII),
                    "en_US".getBytes(US_ASCII));
            writeFrame(responseBody.generateFrame(0));
            state = ConnectionState.AWAIT_START_OK;
            bufferSender.flush();
        } catch (QpidException e) {
            log.error("Received unsupported protocol initiation for protocol version: {} ", getProtocolVersion(), e);
        }
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        currentClassId = classId;
        currentMethodId = methodId;
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    void assertState(final ConnectionState requiredState) {
        if (state != requiredState) {
            String replyText = "Command Invalid, expected " + requiredState + " but was " + state;
            sendConnectionClose(ErrorCodes.COMMAND_INVALID, replyText, 0);
            throw new RuntimeException(replyText);
        }
    }

    public void sendConnectionClose(int errorCode, String message, int channelId) {
        sendConnectionClose(channelId, new AMQFrame(0, new ConnectionCloseBody(getProtocolVersion(),
                errorCode, AMQShortString.validValueOf(message), currentClassId, currentMethodId)));
    }

    private void sendConnectionClose(int channelId, AMQFrame frame) {
        if (orderlyClose.compareAndSet(false, true)) {
            // todo mark channel awaiting close ok and close all channels
            writeFrame(frame);
        }
    }

    public synchronized void writeFrame(AMQDataBlock frame) {
        if (log.isDebugEnabled()) {
            log.debug("SEND: " + frame);
        }

        frame.writePayload(bufferSender);
        bufferSender.flush();
    }

    public MethodRegistry getMethodRegistry() {
        return methodRegistry;
    }

    @VisibleForTesting
    public void setBufferSender(ByteBufferSender sender) {
        this.bufferSender = sender;
    }
}
