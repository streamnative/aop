package io.streamnative.pulsar.handlers.amqp.proxy2;

import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ClientMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ProtocolInitiation;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class used to handle client request and send to corresponding brokers,
 * decode and handle response from brokers.
 */
@Slf4j
public class ProxyChannel implements ClientMethodProcessor<ClientChannelMethodProcessor> {

    private Map<String, ProxyBrokerConnection> connectionMap = new ConcurrentHashMap<>();

    @Override
    public void receiveConnectionStart(short versionMajor, short versionMinor, FieldTable serverProperties, byte[] mechanisms, byte[] locales) {
        log.info("xxxx [ProxyClient] receiveConnectionStart");
    }

    @Override
    public void receiveConnectionSecure(byte[] challenge) {
        log.info("xxxx [ProxyClient] receiveConnectionSecure");
    }

    @Override
    public void receiveConnectionRedirect(AMQShortString host, AMQShortString knownHosts) {
        log.info("xxxx [ProxyClient] receiveConnectionRedirect");
    }

    @Override
    public void receiveConnectionTune(int channelMax, long frameMax, int heartbeat) {
        log.info("xxxx [ProxyClient] receiveConnectionTune");
    }

    @Override
    public void receiveConnectionOpenOk(AMQShortString knownHosts) {
        log.info("xxxx [ProxyClient] receiveConnectionOpenOk");
    }

    @Override
    public ProtocolVersion getProtocolVersion() {
        log.info("xxxx [ProxyClient] getProtocolVersion");
        return null;
    }

    @Override
    public ClientChannelMethodProcessor getChannelMethodProcessor(int channelId) {
        log.info("xxxx [ProxyClient] getChannelMethodProcessor");
        return null;
    }

    @Override
    public void receiveConnectionClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        log.info("xxxx [ProxyClient] receiveConnectionClose");
    }

    @Override
    public void receiveConnectionCloseOk() {
        log.info("xxxx [ProxyClient] receiveConnectionCloseOk");
    }

    @Override
    public void receiveHeartbeat() {
        log.info("xxxx [ProxyClient] receiveHeartbeat");
    }

    @Override
    public void receiveProtocolHeader(ProtocolInitiation protocolInitiation) {
        log.info("xxxx [ProxyClient] receiveProtocolHeader");
    }

    @Override
    public void setCurrentMethod(int classId, int methodId) {
        log.info("xxxx [ProxyClient] setCurrentMethod");
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        log.info("xxxx [ProxyClient] ignoreAllButCloseOk");
        return false;
    }

}
