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

import io.streamnative.pulsar.handlers.amqp.AmqpExchange;
import io.streamnative.pulsar.handlers.amqp.ExchangeMessageRouter;
import io.streamnative.pulsar.handlers.amqp.common.exception.AoPServiceRuntimeException.NotSupportedOperationException;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import java.util.concurrent.ExecutorService;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for exchange router creation.
 */
public class ExchangeMessageRouterTest {

    @DataProvider(name = "unsupportedExchangeTypes")
    public Object[][] unsupportedExchangeTypes() {
        return new Object[][]{
                {AmqpExchange.Type.ConsistentHash},
                {AmqpExchange.Type.ModulusHash},
                {AmqpExchange.Type.LocalRandom},
                {AmqpExchange.Type.Random}
        };
    }

    @Test(dataProvider = "unsupportedExchangeTypes")
    public void shouldThrowWhenExchangeTypeHasNoRouter(AmqpExchange.Type exchangeType) {
        PersistentExchange exchange = Mockito.mock(PersistentExchange.class);
        ExecutorService routeExecutor = Mockito.mock(ExecutorService.class);
        Mockito.when(exchange.getType()).thenReturn(exchangeType);

        NotSupportedOperationException exception = Assert.expectThrows(NotSupportedOperationException.class,
                () -> ExchangeMessageRouter.getInstance(exchange, routeExecutor));
        Assert.assertTrue(exception.getMessage().contains(exchangeType.toString()));
    }
}
