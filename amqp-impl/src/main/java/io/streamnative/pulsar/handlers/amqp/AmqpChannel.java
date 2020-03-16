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

import lombok.extern.log4j.Log4j2;
import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AccessRequestOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.BasicContentHeaderProperties;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.v0_8.transport.ServerChannelMethodProcessor;

/**
 * Amqp Channel level method processor.
 */
@Log4j2
public class AmqpChannel implements ServerChannelMethodProcessor {

    protected final AmqpConnection connection;

    private final int channelId;

    public AmqpChannel(AmqpConnection connection, int channelId) {
        this.connection = connection;
        this.channelId = channelId;
    }

    @Override
    public void receiveAccessRequest(AMQShortString realm, boolean exclusive, boolean passive, boolean active,
            boolean write, boolean read) {
        if (log.isDebugEnabled()) {
            log.debug(
                    "RECV[{}] AccessRequest[ realm: {}, exclusive: {}, passive: {}, active: {}, write: {}, read: {} ]",
                    channelId, realm, exclusive, passive, active, write, read);
        }

        MethodRegistry methodRegistry = connection.getMethodRegistry();

        // We don't implement access control class, but to keep clients happy that expect it always use the "0" ticket.
        AccessRequestOkBody response = methodRegistry.createAccessRequestOkBody(0);
        connection.writeFrame(response.generateFrame(channelId));

    }

    @Override
    public void receiveExchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable,
            boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveExchangeDelete(AMQShortString exchange, boolean ifUnused, boolean nowait) {

    }

    @Override
    public void receiveExchangeBound(AMQShortString exchange, AMQShortString routingKey, AMQShortString queue) {

    }

    @Override
    public void receiveQueueDeclare(AMQShortString queue, boolean passive, boolean durable, boolean exclusive,
            boolean autoDelete, boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveQueueBind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
            boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveQueuePurge(AMQShortString queue, boolean nowait) {

    }

    @Override
    public void receiveQueueDelete(AMQShortString queue, boolean ifUnused, boolean ifEmpty, boolean nowait) {

    }

    @Override
    public void receiveQueueUnbind(AMQShortString queue, AMQShortString exchange, AMQShortString bindingKey,
            FieldTable arguments) {

    }

    @Override
    public void receiveBasicRecover(boolean requeue, boolean sync) {

    }

    @Override
    public void receiveBasicQos(long prefetchSize, int prefetchCount, boolean global) {

    }

    @Override
    public void receiveBasicConsume(AMQShortString queue, AMQShortString consumerTag, boolean noLocal, boolean noAck,
            boolean exclusive, boolean nowait, FieldTable arguments) {

    }

    @Override
    public void receiveBasicCancel(AMQShortString consumerTag, boolean noWait) {

    }

    @Override
    public void receiveBasicPublish(AMQShortString exchange, AMQShortString routingKey, boolean mandatory,
            boolean immediate) {

    }

    @Override
    public void receiveBasicGet(AMQShortString queue, boolean noAck) {

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

    @Override
    public void receiveBasicReject(long deliveryTag, boolean requeue) {

    }

    @Override
    public void receiveTxSelect() {

    }

    @Override
    public void receiveTxCommit() {

    }

    @Override
    public void receiveTxRollback() {

    }

    @Override
    public void receiveConfirmSelect(boolean nowait) {

    }
}
