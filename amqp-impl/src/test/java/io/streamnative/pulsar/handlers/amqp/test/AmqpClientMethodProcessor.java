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
package io.streamnative.pulsar.handlers.amqp.test;

import io.streamnative.pulsar.handlers.amqp.extension.ExtensionClientChannelMethodProcessor;
import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.HeartbeatBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

/**
 * Client connection level method processor for testing.
 */
@Log4j2
public class AmqpClientMethodProcessor implements ClientMethodProcessor<ExtensionClientChannelMethodProcessor> {

    final AmqpClientChannel clientChannel;
    final MethodRegistry methodRegistry;

    public AmqpClientMethodProcessor(AmqpClientChannel clientChannel) {
        this.clientChannel = clientChannel;
        this.methodRegistry = new MethodRegistry(ProtocolVersion.v0_91);
    }


    @Override
    public void receiveConnectionStart(short versionMajor, short versionMinor, FieldTable serverProperties,
                                       byte[] mechanisms, byte[] locales) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION START] - version major {} - version minor {}", versionMajor,
                    versionMinor);
        }
        clientChannel.add(methodRegistry.createConnectionStartBody(versionMajor, versionMinor, serverProperties,
                mechanisms, locales));
    }

    @Override
    public void receiveConnectionSecure(byte[] challenge) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION SECURE] - challenge {}", challenge);
        }
        clientChannel.add(methodRegistry.createConnectionSecureBody(challenge));
    }

    @Override
    public void receiveConnectionRedirect(AMQShortString host, AMQShortString knownHosts) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION REDIRECT] - host {} - knownHosts {}", host, knownHosts);
        }
        clientChannel.add(methodRegistry.createConnectionRedirectBody(host, knownHosts));
    }

    @Override
    public void receiveConnectionTune(int channelMax, long frameMax, int heartbeat) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION TUNE] - channelMax {} - frameMax {} - heartbeat {}", channelMax,
                    frameMax, heartbeat);
        }
        clientChannel.add(methodRegistry.createConnectionTuneBody(channelMax, frameMax, heartbeat));
    }

    @Override
    public void receiveConnectionOpenOk(AMQShortString knownHosts) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION OPEN OK] - knownHosts {}", knownHosts);
        }
        clientChannel.add(methodRegistry.createConnectionOpenOkBody(knownHosts));
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return ProtocolVersion.v0_91;
    }

    @Override
    public ExtensionClientChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        return (ExtensionClientChannelMethodProcessor) new AmqpClientChannelMethodProcessor(this);
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION CLOSE] - replyCode {} - replyText {} - classId {} - methodId {}",
                    replyCode, replyText, classId, methodId);
        }
        clientChannel.add(methodRegistry.createConnectionCloseBody(replyCode, replyText, classId, methodId));
    }

    @Override
    public void receiveConnectionCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONNECTION CLOSE OK]");
        }
        clientChannel.add(methodRegistry.createConnectionCloseOkBody());
    }

    @Override
    public void receiveHeartbeat() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE HEARTBEAT]");
        }
        clientChannel.add(new HeartbeatBody());
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE PROTOCOL HEADER] - {}", protocolInitiation);
        }
        clientChannel.add(protocolInitiation);
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }
}
