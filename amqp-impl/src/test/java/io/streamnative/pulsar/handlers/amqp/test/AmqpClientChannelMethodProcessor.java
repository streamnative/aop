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

import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicAckBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;
import org.apache.qpid.server.protocol.v0_8.transport.ConfirmSelectOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;
import org.apache.qpid.server.protocol.v0_8.transport.ContentHeaderBody;

/**
 * Client channel level method processor for testing.
 */
@Log4j2
public class AmqpClientChannelMethodProcessor implements ClientChannelMethodProcessor {

    private final AmqpClientMethodProcessor clientMethodProcessor;

    public AmqpClientChannelMethodProcessor(AmqpClientMethodProcessor clientMethodProcessor) {
        this.clientMethodProcessor = clientMethodProcessor;
    }

    @Override
    public void receiveChannelOpenOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL OPEN OK]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createChannelOpenOkBody());
    }

    @Override
    public void receiveChannelAlert(int replyCode, AMQShortString replyText, FieldTable details) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL ALERT - replyCode {} - replyText {} - details {}]", replyCode, replyText,
                    details);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createChannelAlertBody(replyCode, replyText, details));
    }

    @Override
    public void receiveAccessRequestOk(int ticket) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE ACCESS REQUEST OK] - ticket {}", ticket);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createAccessRequestOkBody(ticket));
    }

    @Override
    public void receiveExchangeDeclareOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE EXCHANGE DECLARE OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createExchangeDeclareOkBody());
    }

    @Override
    public void receiveExchangeDeleteOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE EXCHANGE DELETE OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createExchangeDeleteOkBody());
    }

    @Override
    public void receiveExchangeBoundOk(int replyCode, AMQShortString replyText) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE EXCHANGE BOUND OK] - replyCode {} - replyText {}", replyCode, replyText);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createExchangeBoundOkBody(replyCode, replyText));
    }

    @Override
    public void receiveQueueBindOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE QUEUE BIND OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createQueueBindOkBody());
    }

    @Override
    public void receiveQueueUnbindOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE QUEUE UNBIND OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createQueueUnbindOkBody());
    }

    @Override
    public void receiveQueueDeclareOk(AMQShortString queue, long messageCount, long consumerCount) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE QUEUE DECLARE OK] - queue {} - messageCount {} - consumerCount {}", queue,
                    messageCount, consumerCount);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createQueueDeclareOkBody(queue, messageCount, consumerCount));
    }

    @Override
    public void receiveQueuePurgeOk(long messageCount) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE QUEUE PURGE OK] - messageCount {}", messageCount);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createQueuePurgeOkBody(messageCount));
    }

    @Override
    public void receiveQueueDeleteOk(long messageCount) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE QUEUE DELETE OK] - messageCount {}", messageCount);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createQueueDeleteOkBody(messageCount));
    }

    @Override
    public void receiveBasicRecoverSyncOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC RECOVER SYNC OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicRecoverSyncOkBody());
    }

    @Override
    public void receiveBasicQosOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC QOS OK]");
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicQosOkBody());
    }

    @Override
    public void receiveBasicConsumeOk(AMQShortString consumerTag) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC CONSUME OK] - consumerTag {}", consumerTag);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicConsumeOkBody(consumerTag));
    }

    @Override
    public void receiveBasicCancelOk(AMQShortString consumerTag) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC CANCEL OK] - consumerTag {}", consumerTag);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicCancelOkBody(consumerTag));
    }

    @Override
    public void receiveBasicReturn(int replyCode, AMQShortString replyText, AMQShortString exchange,
                                   AMQShortString routingKey) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC RETURN] - replyCode {} - replyText {} - exchange {} - routingKey {}",
                    replyCode, replyText, exchange, routingKey);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicReturnBody(replyCode, replyText, exchange, routingKey));
    }

    @Override
    public void receiveBasicDeliver(AMQShortString consumerTag, long deliveryTag, boolean redelivered,
                                    AMQShortString exchange, AMQShortString routingKey) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC DELIVER] - consumerTag {} - deliveryTag {} - redelivered {} - exchange {} "
                    + "- routingKey {}", consumerTag, deliveryTag, redelivered, exchange, routingKey);
        }
        clientMethodProcessor.clientChannel.add(
                clientMethodProcessor.methodRegistry.createBasicDeliverBody(consumerTag, deliveryTag, redelivered,
                        exchange, routingKey));
    }

    @Override
    public void receiveBasicGetOk(long deliveryTag, boolean redelivered, AMQShortString exchange,
                                  AMQShortString routingKey, long messageCount) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC GET OK] - deliveryTag: {} - redelivered: {} - exchange: {} - "
                    + "routingKey: {} - messageCount{}", deliveryTag, redelivered, exchange, routingKey, messageCount);
        }

        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createBasicGetOkBody(deliveryTag,
                redelivered, exchange, routingKey, messageCount));
    }

    @Override
    public void receiveBasicGetEmpty() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC GET EMPTY]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.
                createBasicGetEmptyBody(AMQShortString.EMPTY_STRING));
    }

    @Override
    public void receiveTxSelectOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE TX SELECT OK]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createTxSelectOkBody());
    }

    @Override
    public void receiveTxCommitOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE TX COMMIT OK]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createTxCommitOkBody());
    }

    @Override
    public void receiveTxRollbackOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE TX ROLLBACK OK]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createTxRollbackOkBody());
    }

    @Override
    public void receiveConfirmSelectOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CONFIRM SELECT OK]");
        }
        clientMethodProcessor.clientChannel.add(ConfirmSelectOkBody.INSTANCE);
    }

    @Override
    public void receiveChannelFlow(boolean active) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL FLOW] - active {}", active);
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createChannelFlowBody(active));
    }

    @Override
    public void receiveChannelFlowOk(boolean active) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL FLOW OK] - active {}", active);
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createChannelFlowOkBody(active));
    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL CLOSE] - replyCode {} - replyText {} - classId {} - methodId {}",
                    replyCode, replyText, classId, methodId);
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createChannelCloseBody(replyCode,
                replyText, classId, methodId));
    }

    @Override
    public void receiveChannelCloseOk() {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE CHANNEL CLOSE OK]");
        }
        clientMethodProcessor.clientChannel.add(clientMethodProcessor.methodRegistry.createChannelCloseOkBody());
    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE MESSAGE CONTENT] - size {}", data.remaining());
        }
        clientMethodProcessor.clientChannel.add(new ContentBody(data));
    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE MESSAGE HEADER] - properties {} - bodySize {}", properties, bodySize);
        }
        clientMethodProcessor.clientChannel.add(new ContentHeaderBody(properties, bodySize));
    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC NACK] - deliveryTag {} - multiple {} - requeue {}", deliveryTag, multiple,
                    requeue);
        }
    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {
        if (log.isDebugEnabled()) {
            log.debug("[RECEIVE BASIC ACK] - deliveryTag {} - multiple {}", deliveryTag, multiple);
        }
        clientMethodProcessor.clientChannel.add(new BasicAckBody(deliveryTag, multiple));
    }
}
