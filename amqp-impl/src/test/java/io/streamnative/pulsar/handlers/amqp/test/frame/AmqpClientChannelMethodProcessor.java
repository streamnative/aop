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
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.ClientChannelMethodProcessor;

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

    }

    @Override
    public void receiveChannelAlert(int replyCode, AMQShortString replyText, FieldTable details) {

    }

    @Override
    public void receiveAccessRequestOk(int ticket) {

    }

    @Override
    public void receiveExchangeDeclareOk() {

    }

    @Override
    public void receiveExchangeDeleteOk() {

    }

    @Override
    public void receiveExchangeBoundOk(int replyCode, AMQShortString replyText) {

    }

    @Override
    public void receiveQueueBindOk() {

    }

    @Override
    public void receiveQueueUnbindOk() {

    }

    @Override
    public void receiveQueueDeclareOk(AMQShortString queue, long messageCount, long consumerCount) {

    }

    @Override
    public void receiveQueuePurgeOk(long messageCount) {

    }

    @Override
    public void receiveQueueDeleteOk(long messageCount) {

    }

    @Override
    public void receiveBasicRecoverSyncOk() {

    }

    @Override
    public void receiveBasicQosOk() {

    }

    @Override
    public void receiveBasicConsumeOk(AMQShortString consumerTag) {

    }

    @Override
    public void receiveBasicCancelOk(AMQShortString consumerTag) {

    }

    @Override
    public void receiveBasicReturn(int replyCode, AMQShortString replyText, AMQShortString exchange,
                                   AMQShortString routingKey) {

    }

    @Override
    public void receiveBasicDeliver(AMQShortString consumerTag, long deliveryTag, boolean redelivered,
                                    AMQShortString exchange, AMQShortString routingKey) {

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

    }

    @Override
    public void receiveTxSelectOk() {

    }

    @Override
    public void receiveTxCommitOk() {

    }

    @Override
    public void receiveTxRollbackOk() {

    }

    @Override
    public void receiveConfirmSelectOk() {

    }

    @Override
    public void receiveChannelFlow(boolean active) {

    }

    @Override
    public void receiveChannelFlowOk(boolean active) {

    }

    @Override
    public void receiveChannelClose(int replyCode, AMQShortString replyText, int classId, int methodId) {

    }

    @Override
    public void receiveChannelCloseOk() {

    }

    @Override
    public void receiveMessageContent(QpidByteBuffer data) {

    }

    @Override
    public void receiveMessageHeader(BasicContentHeaderProperties properties, long bodySize) {

    }

    @Override
    public boolean ignoreAllButCloseOk() {
        return false;
    }

    @Override
    public void receiveBasicNack(long deliveryTag, boolean multiple, boolean requeue) {

    }

    @Override
    public void receiveBasicAck(long deliveryTag, boolean multiple) {

    }
}
