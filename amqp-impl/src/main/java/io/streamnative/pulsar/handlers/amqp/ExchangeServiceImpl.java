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

import static org.apache.qpid.server.protocol.ErrorCodes.INTERNAL_ERROR;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.AMQMethodBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeDeleteOkBody;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;

/**
 * Logic of exchange.
 */
@Slf4j
public class ExchangeServiceImpl implements ExchangeService {
    private ExchangeContainer exchangeContainer;

    public ExchangeServiceImpl(ExchangeContainer exchangeContainer) {
        this.exchangeContainer = exchangeContainer;
    }

    private void handleDefaultExchangeInExchangeDeclare(AmqpChannel channel, AMQShortString exchange) {
        if (isDefaultExchange(exchange)) {
            StringBuffer sb = new StringBuffer();
            sb.append("Attempt to redeclare default exchange: of type ")
                    .append(ExchangeDefaults.DIRECT_EXCHANGE_CLASS);
            channel.closeChannel(ErrorCodes.ACCESS_REFUSED, sb.toString());
        }
    }

    private String formatString(String s) {
        return s.replaceAll("\r", "").
                replaceAll("\n", "").trim();
    }


    public void exchangeDeclare(AmqpChannel channel, AMQShortString exchange, AMQShortString type,
                                boolean passive, boolean durable, boolean autoDelete,
                                boolean internal, boolean nowait, FieldTable arguments) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDeclare[ exchange: {},"
                            + " type: {}, passive: {}, durable: {}, autoDelete: {}, internal: {}, "
                            + "nowait: {}, arguments: {} ]", channelId, exchange,
                    type, passive, durable, autoDelete, internal, nowait, arguments);
        }

        handleDefaultExchangeInExchangeDeclare(channel, exchange);

        String exchangeName = formatString(exchange.toString());
        final MethodRegistry methodRegistry = connection.getMethodRegistry();
        final AMQMethodBody declareOkBody = methodRegistry.createExchangeDeclareOkBody();
        boolean createIfMissing = passive ? false : true;
        String exchangeType = type.toString();
        if (channel.isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = channel.getExchangeType(exchange.toString());
        }
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                exchangeContainer.asyncGetExchange(connection.getPulsarService(), connection.getNamespaceName(),
                        exchangeName, createIfMissing, exchangeType);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                connection.sendConnectionClose(ErrorCodes.NOT_FOUND, "Unknown exchange: " + exchangeName, channelId);
            } else {
                if (null == amqpExchange) {
                    channel.closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange:" + exchangeName);
                } else {
                    if (!(type == null || type.length() == 0)
                            && !amqpExchange.getType().toString().equalsIgnoreCase(type.toString())) {
                        connection.sendConnectionClose(ErrorCodes.NOT_ALLOWED,
                                "Attempt to redeclare exchange: '"
                                        + exchangeName + "' of type " + amqpExchange.getType()
                                        + " to " + type + ".", channelId);
                    } else if (!nowait) {
                        channel.sync();
                        connection.writeFrame(declareOkBody.generateFrame(channelId));
                    }
                }
            }
        });
    }

    @Override
    public void exchangeDelete(AmqpChannel channel, AMQShortString exchange, boolean ifUnused, boolean nowait) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeDelete[ exchange: {}, ifUnused: {}, nowait:{} ]", channelId, exchange, ifUnused,
                    nowait);
        }
        if (isDefaultExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "Default Exchange cannot be deleted. ",
                    channelId);
        } else if (isBuildInExchange(exchange)) {
            connection.sendConnectionClose(ErrorCodes.ACCESS_REFUSED, "BuildIn Exchange cannot be deleted. ",
                    channelId);
        } else {
            String exchangeName = formatString(exchange.toString());
            CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                    exchangeContainer.asyncGetExchange(connection.getPulsarService(), connection.getNamespaceName(),
                            exchangeName, false,
                            null);
            amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
                if (throwable != null) {
                    log.error("Get Topic error:{}", throwable.getMessage());
                } else {
                    if (null == amqpExchange) {
                        channel.closeChannel(ErrorCodes.NOT_FOUND, "Unknown exchange: '" + exchangeName + "'");
                    } else {
                        PersistentTopic topic = (PersistentTopic) amqpExchange.getTopic();
                        if (ifUnused && topic.getSubscriptions().isEmpty()) {
                            channel.closeChannel(ErrorCodes.IN_USE, "Exchange has bindings. ");
                        } else {
                            try {
                                exchangeContainer.deleteExchange(connection.getNamespaceName(), exchangeName);
                                topic.delete().get();
                                ExchangeDeleteOkBody responseBody = connection.getMethodRegistry().
                                        createExchangeDeleteOkBody();
                                connection.writeFrame(responseBody.generateFrame(channelId));
                            } catch (Exception e) {
                                connection.sendConnectionClose(INTERNAL_ERROR,
                                        "Catch a PulsarAdminException: " + e.getMessage()
                                                + ". channelId: ", channelId);
                            }
                        }
                    }
                }
            });
        }
    }

    @Override
    public void exchangeBound(AmqpChannel channel, AMQShortString exchange, AMQShortString routingKey,
                              AMQShortString queueName) {
        int channelId = channel.getChannelId();
        AmqpConnection connection = channel.getConnection();
        if (log.isDebugEnabled()) {
            log.debug("RECV[{}] ExchangeBound[ exchange: {}, routingKey: {}, queue:{} ]", channelId, exchange,
                    routingKey, queueName);
        }

        String exchangeName = formatString(exchange.toString());
        CompletableFuture<AmqpExchange> amqpExchangeCompletableFuture =
                exchangeContainer.asyncGetExchange(connection.getPulsarService(), connection.getNamespaceName(),
                        exchangeName, false,
                        null);
        amqpExchangeCompletableFuture.whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Get Topic error:{}", throwable.getMessage());
                connection.sendConnectionClose(ErrorCodes.NOT_FOUND, "Unknown exchange: " + exchangeName, channelId);
            } else {
                int replyCode;
                StringBuilder replyText = new StringBuilder();
                if (null == amqpExchange) {
                    replyCode = ExchangeBoundOkBody.EXCHANGE_NOT_FOUND;
                    replyText = replyText.insert(0, "Exchange '").append(exchange).append("' not found");
                } else {
                    List<String> subs = amqpExchange.getTopic().getSubscriptions().keys();
                    if (null == subs || subs.isEmpty()) {
                        replyCode = ExchangeBoundOkBody.QUEUE_NOT_FOUND;
                        replyText = replyText.insert(0, "Queue '").append(queueName).append("' not found");
                    } else {
                        replyCode = ExchangeBoundOkBody.OK;
                        replyText = null;
                    }
                }
                MethodRegistry methodRegistry = connection.getMethodRegistry();
                ExchangeBoundOkBody exchangeBoundOkBody = methodRegistry
                        .createExchangeBoundOkBody(replyCode, AMQShortString.validValueOf(replyText));
                connection.writeFrame(exchangeBoundOkBody.generateFrame(channelId));
            }
        });
    }

    private boolean isDefaultExchange(final AMQShortString exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.equals(exchangeName);
    }

    private boolean isBuildInExchange(final AMQShortString exchangeName) {
        if (exchangeName.toString().equals(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
                || (exchangeName.toString().equals(ExchangeDefaults.FANOUT_EXCHANGE_NAME))
                || (exchangeName.toString().equals(ExchangeDefaults.TOPIC_EXCHANGE_NAME))) {
            return true;
        } else {
            return false;
        }
    }

}
