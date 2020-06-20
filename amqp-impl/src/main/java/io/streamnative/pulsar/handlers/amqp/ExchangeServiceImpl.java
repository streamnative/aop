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

import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;

@Slf4j
public class ExchangeServiceImpl implements ExchangeService {
    private final int channelId;
    private final AmqpConnection connection;
    private AmqpChannel amqpChannel;

    public ExchangeServiceImpl(AmqpChannel amqpChannel) {
        this.amqpChannel = amqpChannel;
        this.channelId = amqpChannel.getChannelId();
        this.connection = amqpChannel.getConnection();
    }

    private void handleDefaultExchangeInExchangeDeclare(AMQShortString exchange) {
        if (isDefaultExchange(exchange)) {
            StringBuffer sb = new StringBuffer();
            sb.append("Attempt to redeclare default exchange: of type ")
                    .append(ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
            amqpChannel.closeChannel(ErrorCodes.ACCESS_REFUSED, sb.toString());
        }
    }

    private String formatString(String s) {
        return s.replaceAll("\r", "").
                replaceAll("\n", "").trim();
    }


    public void exchangeDeclare(AMQShortString exchange, AMQShortString type, boolean passive, boolean durable, boolean autoDelete, boolean internal, boolean nowait, FieldTable arguments) {
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDeclare[ exchange: {},"
                            + " type: {}, passive: {}, durable: {}, autoDelete: {}, internal: {}, "
                            + "nowait: {}, arguments: {} ]", channelId, exchange,
                    type, passive, durable, autoDelete, internal, nowait, arguments);
        }

        handleDefaultExchangeInExchangeDeclare(exchange);

        String exchangeName = formatString(exchange.toString());
        final MethodRegistry methodRegistry = connection.getMethodRegistry();
        final AMQMethodBody declareOkBody = methodRegistry.createExchangeDeclareOkBody();
        boolean createIfMissing = passive ? false : true;

        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                ExchangeContainer.asyncGetExchange(connection.getNamespaceName(),
                        exchangeName, createIfMissing,
                        type.toString());
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
            } else {
                if (null == amqpExchange) {
                    amqpChannel.closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + exchangeName + "'");
                    return;
                } else {
                    if (!(type == null || type.length() == 0)
                            && !amqpExchange.getType().toString().equalsIgnoreCase(type.toString())) {
                        connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED, "Attempt to redeclare exchange: '"
                                + exchangeName + "' of type " + amqpExchange.getType() + " to " + type + ".", channelId);
                    } else if (!nowait) {
                        amqpChannel.sync();
                        connection.writeFrame(declareOkBody.generateFrame(channelId));
                    }
                }
            }
        });
    }

    @Override
    public void exchangeDelete() {

    }

    @Override
    public void exchangeBound() {

    }

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

}
