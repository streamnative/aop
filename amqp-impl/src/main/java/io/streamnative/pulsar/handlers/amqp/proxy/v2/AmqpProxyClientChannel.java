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
package io.streamnative.pulsar.handlers.amqp.proxy.v2;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicConsumeOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ChannelCloseOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueBindOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueDeclareOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.QueueUnbindOkBody;


@Slf4j
public class AmqpProxyClientChannel implements ClientChannelMethodProcessor {

    private final Integer channelId;
    private final ProxyBrokerConnection conn;

    public AmqpProxyClientChannel(Integer channelId, ProxyBrokerConnection conn) {
        this.channelId = channelId;
        this.conn = conn;
    }

    @Override
    public void receiveChannelOpenOk() {
        conn.getChannelState().put(channelId, ProxyBrokerConnection.ChannelState.OPEN);
        conn.getProxyConnection().getServerChannelMap().get(channelId).initComplete();
    }

    @Override
    public void receiveChannelAlert(int replyCode, AMQShortString replyText, FieldTable details) {
        // nothing to do
    }

    @Override
    public void receiveAccessRequestOk(int ticket) {
        // nothing to do
    }

    @Override
    public void receiveExchangeDeclareOk() {
        conn.getClientChannel().writeAndFlush(new ExchangeDeclareOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveExchangeDeleteOk() {
        // nothing to do
    }

    @Override
    public void receiveExchangeBoundOk(int replyCode, AMQShortString replyText) {
        // nothing to do
    }

    @Override
    public void receiveQueueBindOk() {
        conn.getClientChannel().writeAndFlush(new QueueBindOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveQueueUnbindOk() {
        conn.getClientChannel().writeAndFlush(new QueueUnbindOkBody().generateFrame(channelId));
    }

    @Override
    public void receiveQueueDeclareOk(AMQShortString queue, long messageCount, long consumerCount) {
        conn.getClientChannel().writeAndFlush(
                new QueueDeclareOkBody(queue, messageCount, consumerCount).generateFrame(channelId));
    }

    @Override
    public void receiveQueuePurgeOk(long messageCount) {
        // nothing to do
    }

    @Override
    public void receiveQueueDeleteOk(long messageCount) {
        // nothing to do
    }

    @Override
    public void receiveBasicRecoverSyncOk() {
        // nothing to do
    }

    @Override
    public void receiveBasicQosOk() {
        // nothing to do
    }

    @Override
    public void receiveBasicConsumeOk(AMQShortString consumerTag) {
        conn.getClientChannel().writeAndFlush(new BasicConsumeOkBody(consumerTag).generateFrame(channelId));
    }

    @Override
    public void receiveBasicCancelOk(AMQShortString consumerTag) {
        // nothing to do
    }

    @Override
    public void receiveBasicReturn(int replyCode, AMQShortString replyText, AMQShortString exchange,
                                   AMQShortString routingKey) {
        // nothing to do
    }

    @Override
    public void receiveBasicDeliver(AMQShortString consumerTag, long deliveryTag, boolean redelivered,
                                    AMQShortString exchange, AMQShortString routingKey) {
        // nothing to do
    }

    @Override
    public void receiveBasicGetOk(long deliveryTag, boolean redelivered, AMQShortString exchange,
                                  AMQShortString routingKey, long messageCount) {
        // nothing to do
    }

    @Override
    public void receiveBasicGetEmpty() {
        // nothing to do
    }

    @Override
    public void receiveTxSelectOk() {
        // nothing to do
    }

    @Override
    public void receiveTxCommitOk() {
        // nothing to do
    }

    @Override
    public void receiveTxRollbackOk() {
        // nothing to do
    }

    @Override
    public void receiveConfirmSelectOk() {
        // nothing to do
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        // nothing to do
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        // nothing to do
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        // nothing to do
    }

    @Override
    public void receiveChannelCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("ProxyClientChannel receive channel close ok request.");
        }
        conn.getChannelState().put(channelId, ProxyBrokerConnection.ChannelState.CLOSE);
        AtomicBoolean allClose = new AtomicBoolean(true);
        conn.getProxyConnection().getConnectionMap().values().forEach(conn -> {
            if (conn.getChannelState().getOrDefault(channelId, null)
                    == ProxyBrokerConnection.ChannelState.OPEN) {
                allClose.set(false);
            }
        });
        if (allClose.get()) {
            conn.getProxyConnection().writeFrame(ChannelCloseOkBody.INSTANCE.generateFrame(channelId));
        }
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        // nothing to do
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        // nothing to do
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        // nothing to do
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        // nothing to do
    }

}
