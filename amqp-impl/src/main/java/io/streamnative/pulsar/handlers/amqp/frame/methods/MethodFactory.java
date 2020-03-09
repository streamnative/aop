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
package io.streamnative.pulsar.handlers.amqp.frame.methods;


import io.netty.buffer.ByteBuf;
import io.streamnative.pulsar.handlers.amqp.frame.connection.Connection;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionClose;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionCloseOk;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionOpenOk;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionRedirect;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionSecure;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionStart;
import io.streamnative.pulsar.handlers.amqp.frame.connection.ConnectionTune;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.Basic;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicCancelOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicConsumeOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicDeliver;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicGetEmpty;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicGetOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicQosOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.basic.BasicReturn;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.Channel;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.ChannelClose;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.ChannelCloseOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.ChannelFlow;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.ChannelFlowOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.channel.ChannelOpenOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.exchange.Exchange;
import io.streamnative.pulsar.handlers.amqp.frame.methods.exchange.ExchangeDeclareOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.exchange.ExchangeDeleteOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.queue.Queue;
import io.streamnative.pulsar.handlers.amqp.frame.methods.queue.QueueBindOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.queue.QueueDeclareOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.queue.QueueDeleteOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.queue.QueuePurgeOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.tx.Tx;
import io.streamnative.pulsar.handlers.amqp.frame.methods.tx.TxCommitOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.tx.TxRollbackOk;
import io.streamnative.pulsar.handlers.amqp.frame.methods.tx.TxSelectOk;
import io.streamnative.pulsar.handlers.amqp.frame.types.UnsignedShort;

/**
 * MethodFactory to generate method by classId and methodId.
 */
public final class MethodFactory {

    private MethodFactory() {
    }

    public static Method get(UnsignedShort classId, UnsignedShort methodId, ByteBuf channelBuffer) throws Exception {

        if (classId.equals(Connection.CLASS_ID)) {
            if (methodId.equals(ConnectionStart.METHOD_ID)) {
                return new ConnectionStart(channelBuffer);
            }

            if (methodId.equals(ConnectionSecure.METHOD_ID)) {
                return new ConnectionSecure(channelBuffer);
            }

            if (methodId.equals(ConnectionTune.METHOD_ID)) {
                return new ConnectionTune(channelBuffer);
            }

            if (methodId.equals(ConnectionOpenOk.METHOD_ID)) {
                return new ConnectionOpenOk(channelBuffer);
            }

            if (methodId.equals(ConnectionRedirect.METHOD_ID)) {
                return new ConnectionRedirect(channelBuffer);
            }

            if (methodId.equals(ConnectionClose.METHOD_ID)) {
                return new ConnectionClose(channelBuffer);
            }

            if (methodId.equals(ConnectionCloseOk.METHOD_ID)) {
                return new ConnectionCloseOk(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else if (classId.equals(Channel.CLASS_ID)) {
            if (methodId.equals(ChannelOpenOk.METHOD_ID)) {
                return new ChannelOpenOk(channelBuffer);
            }

            if (methodId.equals(ChannelFlow.METHOD_ID)) {
                return new ChannelFlow(channelBuffer);
            }

            if (methodId.equals(ChannelFlowOk.METHOD_ID)) {
                return new ChannelFlowOk(channelBuffer);
            }

            if (methodId.equals(ChannelClose.METHOD_ID)) {
                return new ChannelClose(channelBuffer);
            }

            if (methodId.equals(ChannelCloseOk.METHOD_ID)) {
                return new ChannelCloseOk(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else if (classId.equals(Exchange.CLASS_ID)) {
            if (methodId.equals(ExchangeDeclareOk.METHOD_ID)) {
                return new ExchangeDeclareOk(channelBuffer);
            }

            if (methodId.equals(ExchangeDeleteOk.METHOD_ID)) {
                return new ExchangeDeleteOk(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else if (classId.equals(Queue.CLASS_ID)) {
            if (methodId.equals(QueueDeclareOk.METHOD_ID)) {
                return new QueueDeclareOk(channelBuffer);
            }

            if (methodId.equals(QueueBindOk.METHOD_ID)) {
                return new QueueBindOk(channelBuffer);
            }

            if (methodId.equals(QueuePurgeOk.METHOD_ID)) {
                return new QueuePurgeOk(channelBuffer);
            }

            if (methodId.equals(QueueDeleteOk.METHOD_ID)) {
                return new QueueDeleteOk(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else if (classId.equals(Basic.CLASS_ID)) {

            if (methodId.equals(BasicQosOk.METHOD_ID)) {
                return new BasicQosOk(channelBuffer);
            }

            if (methodId.equals(BasicConsumeOk.METHOD_ID)) {
                return new BasicConsumeOk(channelBuffer);
            }

            if (methodId.equals(BasicCancelOk.METHOD_ID)) {
                return new BasicCancelOk(channelBuffer);
            }

            if (methodId.equals(BasicReturn.METHOD_ID)) {
                return new BasicReturn(channelBuffer);
            }

            if (methodId.equals(BasicDeliver.METHOD_ID)) {
                return new BasicDeliver(channelBuffer);
            }

            if (methodId.equals(BasicGetOk.METHOD_ID)) {
                return new BasicGetOk(channelBuffer);
            }

            if (methodId.equals(BasicGetEmpty.METHOD_ID)) {
                return new BasicGetEmpty(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else if (classId.equals(Tx.CLASS_ID)) {

            if (methodId.equals(TxSelectOk.METHOD_ID)) {
                return new TxSelectOk(channelBuffer);
            }

            if (methodId.equals(TxCommitOk.METHOD_ID)) {
                return new TxCommitOk(channelBuffer);
            }

            if (methodId.equals(TxRollbackOk.METHOD_ID)) {
                return new TxRollbackOk(channelBuffer);
            }

            throw new UnknownMethodException(classId, methodId);
        } else {
            throw new UnknownClassException(classId);
        }
    }
}
