package io.streamnative.pulsar.handlers.amqp.proxy2;

import io.streamnative.pulsar.handlers.amqp.proxy.PulsarServiceLookupHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareBody;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class ProxyServerChannel implements ServerChannelMethodProcessor {

    private Integer channelId;
    private ProxyConnection proxyConnection;

    public ProxyServerChannel(Integer channelId, ProxyConnection proxyConnection) {
        this.channelId = channelId;
        this.proxyConnection = proxyConnection;
    }

    public CompletableFuture<ProxyBrokerConnection> getConn(String topic) {
        return proxyConnection.getBrokerConnection(topic);
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active, boolean write, boolean read) {
        log.info("xxxx [ProxyServerChannel] receiveAccessRequest");
    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        log.info("xxxx [ProxyServerChannel] receiveExchangeDeclare");
        getConn(exchange.toString()).thenAccept(conn -> {
            ExchangeDeclareBody exchangeDeclareBody = new ExchangeDeclareBody(0, exchange, type, passive, durable, autoDelete, internal, nowait, arguments);
            conn.writeFrame(exchangeDeclareBody.generateFrame(channelId));
        });
    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {
        log.info("xxxx [ProxyServerChannel] receiveExchangeDelete");
    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {
        log.info("xxxx [ProxyServerChannel] receiveExchangeBound");
    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive, boolean autoDelete, boolean nowait, FieldTable arguments) {
        log.info("xxxx [ProxyServerChannel] receiveQueueDeclare");
    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, boolean nowait, FieldTable arguments) {
        log.info("xxxx [ProxyServerChannel] receiveQueueBind");
    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {
        log.info("xxxx [ProxyServerChannel] receiveQueuePurge");
    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {
        log.info("xxxx [ProxyServerChannel] receiveQueueDelete");
    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey, FieldTable arguments) {
        log.info("xxxx [ProxyServerChannel] receiveQueueUnbind");
    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {
        log.info("xxxx [ProxyServerChannel] receiveBasicRecover");
    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {
        log.info("xxxx [ProxyServerChannel] receiveBasicQos");
    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck, boolean exclusive, boolean nowait, FieldTable arguments) {
        log.info("xxxx [ProxyServerChannel] receiveBasicConsume");
    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {
        log.info("xxxx [ProxyServerChannel] receiveBasicCancel");
    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory, boolean immediate) {
        log.info("xxxx [ProxyServerChannel] receiveBasicPublish");
    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {
        log.info("xxxx [ProxyServerChannel] receiveBasicGet");
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        log.info("xxxx [ProxyServerChannel] receiveBasicAck");
    }

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {
        log.info("xxxx [ProxyServerChannel] receiveBasicReject");
    }

    @Override
    public void receiveTxSelect() {
        log.info("xxxx [ProxyServerChannel] receiveTxSelect");
    }

    @Override
    public void receiveTxCommit() {
        log.info("xxxx [ProxyServerChannel] receiveTxCommit");
    }

    @Override
    public void receiveTxRollback() {
        log.info("xxxx [ProxyServerChannel] receiveTxRollback");
    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {
        log.info("xxxx [ProxyServerChannel] receiveConfirmSelect");
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        log.info("xxxx [ProxyServerChannel] receiveChannelFlow");
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        log.info("xxxx [ProxyServerChannel] receiveChannelFlowOk");
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        log.info("xxxx [ProxyServerChannel] receiveChannelClose");
    }

    @Override
    public void receiveChannelCloseOk() {
        log.info("xxxx [ProxyServerChannel] receiveChannelCloseOk");
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        log.info("xxxx [ProxyServerChannel] receiveMessageContent");
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        log.info("xxxx [ProxyServerChannel] receiveMessageHeader");
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        log.info("xxxx [ProxyServerChannel] ignoreAllButCloseOk");
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        log.info("xxxx [ProxyServerChannel] receiveBasicNack");
    }

}
