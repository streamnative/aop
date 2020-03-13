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
package io.streamnative.pulsar.handlers.amqp.test.frame;

import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

/**
 * Client connection level method processor for testing.
 */
@Log4j2
public class AmqpClientMethodProcessor implements ClientMethodProcessor<AmqpClientChannelMethodProcessor> {

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
            log.debug("[RECEIVE CONNECTION OPEN OK] - version major {} - version minor {}", versionMajor,
                    versionMinor);
        }
        clientChannel.add(methodRegistry.createConnectionStartBody(versionMajor, versionMinor, serverProperties,
                mechanisms, locales));
    }

    @Override
    public void receiveConnectionSecure(byte[] challenge) {

    }

    @Override
    public void receiveConnectionRedirect(AMQShortString host, AMQShortString knownHosts) {

    }

    @Override
    public void receiveConnectionTune(int channelMax, long frameMax, int heartbeat) {

    }

    @Override
    public void receiveConnectionOpenOk(AMQShortString knownHosts) {

    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        return null;
    }

    @Override
    public AmqpClientChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        return new AmqpClientChannelMethodProcessor(this);
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

    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }
}
