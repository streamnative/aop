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

import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.formatExchangeName;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.getExchangeType;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isBuildInExchange;
import static io.streamnative.pulsar.handlers.amqp.utils.ExchangeUtil.isDefaultExchange;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.ErrorCodes;
import org.apache.qpid.server.protocol.v0_8.FieldTable;
import org.apache.qpid.server.protocol.v0_8.transport.ExchangeBoundOkBody;

/**
 * Logic of exchange.
 */
@Slf4j
public class ExchangeServiceImpl implements ExchangeService {

    private final ExchangeContainer exchangeContainer;

    public ExchangeServiceImpl(ExchangeContainer exchangeContainer) {
        this.exchangeContainer = exchangeContainer;
    }

    @Override
    public CompletableFuture<AmqpExchange> exchangeDeclare(NamespaceName namespaceName, String exchange, String type,
                        boolean passive, boolean durable, boolean autoDelete, boolean internal, FieldTable arguments) {

        if (isDefaultExchange(exchange)) {
            String sb = "Attempt to redeclare default exchange: of type " + ExchangeDefaults.DIRECT_EXCHANGE_CLASS;
            return FutureUtil.failedFuture(new AoPException(ErrorCodes.ACCESS_REFUSED, sb, true, false));
        }

        boolean createIfMissing = !passive;
        String exchangeType;
        if (isBuildInExchange(exchange)) {
            createIfMissing = true;
            exchangeType = getExchangeType(exchange);
        } else {
            exchangeType = type;
        }
        CompletableFuture<AmqpExchange> future = new CompletableFuture<>();
        exchangeContainer.asyncGetExchange(namespaceName, formatExchangeName(exchange), createIfMissing, exchangeType,
                        arguments)
                .whenComplete((ex, throwable) -> {
                    if (throwable != null) {
                        future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND,
                                "Failed to get " + exchange + " in vhost " + namespaceName, false, true));
                        return;
                    }
                    if (ex == null) {
                        future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND,
                                "Get empty exchange " + exchange + " in vhost " + namespaceName, true, false));
                        return;
                    }
                    if (!passive && !ex.getType().toString().equalsIgnoreCase(exchangeType)) {
                        future.completeExceptionally(new AoPException(ErrorCodes.NOT_ALLOWED,
                                "Attempt to redeclare exchange: '" + exchange + "' of type " + ex.getType()
                                        + " to " + exchangeType + ".", false, true));
                        return;
                    }
                    future.complete(ex);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> exchangeDelete(NamespaceName namespaceName, String exchange, boolean ifUnused) {
        if (isDefaultExchange(exchange)) {
            return FutureUtil.failedFuture(new AoPException(
                    ErrorCodes.ACCESS_REFUSED, "Default Exchange [" + exchange + "] cannot be deleted.", false, true));
        }

        if (isBuildInExchange(exchange)) {
            return FutureUtil.failedFuture(new AoPException(
                    ErrorCodes.ACCESS_REFUSED, "BuildIn Exchange [" + exchange + "] cannot be deleted", false, true));
        }

        String exchangeName = formatExchangeName(exchange);
        CompletableFuture<Void> future = new CompletableFuture<>();
        exchangeContainer.asyncGetExchange(namespaceName, exchangeName, false, null)
                .whenComplete((amqpExchange, throwable) -> {
            if (throwable != null) {
                log.error("Failed to get topic {}/{} from container.",
                        namespaceName, exchange, throwable);
                future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND, "Failed to get exchange "
                        + exchange + " from exchange container, vhost is " + namespaceName, false, true));
                return;
            }
            if (null == amqpExchange) {
                future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND,
                        "Unknown exchange: '" + exchangeName + "' in vhost " + namespaceName, true, false));
                return;
            }
            PersistentTopic topic = (PersistentTopic) amqpExchange.getTopic();
            if (ifUnused && !topic.getSubscriptions().isEmpty()) {
                future.completeExceptionally(
                        new AoPException(ErrorCodes.IN_USE, "Exchange " + exchange + " has bindings.", true, false));
                return;
            }
            topic.delete().thenAccept(__ -> {
                exchangeContainer.deleteExchange(namespaceName, exchangeName);
                future.complete(null);
            }).exceptionally(t -> {
                future.completeExceptionally(new AoPException(ErrorCodes.INTERNAL_ERROR,
                        "Failed to delete topic " + exchange + " in vhost " + namespaceName, false, true));
                return null;
            });
        });
        return future;
    }

    @Override
    public CompletableFuture<Integer> exchangeBound(NamespaceName namespaceName, String exchange, String routingKey,
                              String queueName) {
        String exchangeName = formatExchangeName(exchange);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        exchangeContainer.asyncGetExchange(namespaceName, exchangeName, false, null)
                .whenComplete((amqpExchange, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to get topic {}/{} from container.",
                                namespaceName, exchange, throwable);
                        future.completeExceptionally(new AoPException(ErrorCodes.NOT_FOUND,
                                "Failed to get topic " + exchange + " from exchange container, vhost is "
                                        + namespaceName, false, true));
                        return;
                    }
                    int replyCode;
                    if (null == amqpExchange) {
                        replyCode = ExchangeBoundOkBody.EXCHANGE_NOT_FOUND;
                    } else {
                        List<String> subs = amqpExchange.getTopic().getSubscriptions().keys();
                        if (null == subs || subs.isEmpty()) {
                            replyCode = ExchangeBoundOkBody.QUEUE_NOT_FOUND;
                        } else {
                            replyCode = ExchangeBoundOkBody.OK;
                        }
                    }
                    future.complete(replyCode);
                });
        return future;
    }

}
