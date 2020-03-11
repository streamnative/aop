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

import org.apache.pulsar.common.util.collections.ConcurrentLongHashMap;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ServerMethodProcessor;

/**
 * Amqp server level method processor.
 */
public class AmqpServerMethodProcessor implements ServerMethodProcessor<ServerChannelMethodProcessor> {

    private final ConcurrentLongHashMap<ServerChannelMethodProcessor> channelMethodProcessorMap;
    private AmqpChannelInboundHandlerAdapter inboundHandlerAdapter;


    public AmqpServerMethodProcessor(AmqpChannelInboundHandlerAdapter inboundHandlerAdapter) {
        this.channelMethodProcessorMap = new ConcurrentLongHashMap<>();
        this.inboundHandlerAdapter = inboundHandlerAdapter;
    }

    @Override
    public void receiveConnectionStartOk(FieldTable clientProperties, AMQShortString mechanism, byte[] response,
             AMQShortString locale) {

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
        this.channelMethodProcessorMap.put(channelId, new AmqpServerChannelMethodProcessor(this));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return null;
    }

    @Override
    public ServerChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        return this.channelMethodProcessorMap.get(channelId);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {

    }

    @Override
    public void receiveConnectionCloseOk() {

    }

    @Override
    public void receiveHeartbeat() {

    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        inboundHandlerAdapter.getDecoder().setExpectProtocolInitiation(false);
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    public AmqpChannelInboundHandlerAdapter getInboundHandlerAdapter() {
        return inboundHandlerAdapter;
    }
}
