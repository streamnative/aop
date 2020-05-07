


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

package io.streamnative.pulsar.handlers.amqp.rabbitmq.functional;

import static org.junit.Assert.fail;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.test.BrokerTestCase;
import java.io.IOException;
/**
 * Testcase.
 */
public class ExchangeExchangeBindingsAutoDelete extends BrokerTestCase {

    protected void declareExchanges(String[] names) throws IOException {
        for (String e : names) {
            channel.exchangeDeclare(e, "fanout", false, true, null);
        }
    }

    protected void assertExchangesNotExist(String[] names) throws IOException {
        for (String e : names) {
            assertExchangeNotExists(e);
        }
    }

    protected void assertExchangeNotExists(String name) throws IOException {
        try {
            connection.createChannel().exchangeDeclarePassive(name);
            fail("Exchange " + name + " still exists.");
        } catch (IOException e) {
            checkShutdownSignal(AMQP.NOT_FOUND, e);
        }
    }

    /*
     * build (A -> B) and (B -> A) and then delete one binding and both
     * exchanges should autodelete
     */
    //@Test
    public void autoDeleteExchangesSimpleLoop() throws IOException {
        String[] exchanges = new String[]{"A", "B"};
        declareExchanges(exchanges);
        channel.exchangeBind("A", "B", "");
        channel.exchangeBind("B", "A", "");

        channel.exchangeUnbind("A", "B", "");
        assertExchangesNotExist(exchanges);
    }

    /*
     * build (A -> B) (B -> C) (C -> D) and then delete D. All should autodelete
     */
    //@Test
    public void transientAutoDelete() throws IOException {
        String[] exchanges = new String[]{"A", "B", "C", "D"};
        declareExchanges(exchanges);
        channel.exchangeBind("B", "A", "");
        channel.exchangeBind("C", "B", "");
        channel.exchangeBind("D", "C", "");

        channel.exchangeDelete("D");
        assertExchangesNotExist(exchanges);
    }

    /*
     * build (A -> B) (B -> C) (C -> D) (Source -> A) (Source -> B) (Source ->
     * C) (Source -> D) On removal of D, all should autodelete
     */
    //@Test
    public void repeatedTargetAutoDelete() throws IOException {
        String[] exchanges = new String[]{"A", "B", "C", "D"};
        declareExchanges(exchanges);
        channel.exchangeDeclare("Source", "fanout", false, true, null);

        channel.exchangeBind("B", "A", "");
        channel.exchangeBind("C", "B", "");
        channel.exchangeBind("D", "C", "");

        for (String e : exchanges) {
            channel.exchangeBind(e, "Source", "");
        }

        channel.exchangeDelete("A");
        // Source should still be there. We'll verify this by redeclaring
        // it here and verifying it goes away later
        channel.exchangeDeclare("Source", "fanout", false, true, null);

        channel.exchangeDelete("D");
        assertExchangesNotExist(exchanges);
        assertExchangeNotExists("Source");
    }

    /*
     * build (A -> B) (B -> C) (A -> C). Delete C and they should all vanish
     */
    //@Test
    public void autoDeleteBindingToVanishedExchange() throws IOException {
        String[] exchanges = new String[]{"A", "B", "C"};
        declareExchanges(exchanges);
        channel.exchangeBind("C", "B", "");
        channel.exchangeBind("B", "A", "");
        channel.exchangeBind("C", "A", "");
        channel.exchangeDelete("C");
        assertExchangesNotExist(exchanges);
    }

}
