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
package io.streamnative.pulsar.handlers.amqp.utils;

import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;

public class ExchangeUtil {

    public static boolean isBuildInExchange(final String exchangeName) {
        return exchangeName.equals(ExchangeDefaults.DIRECT_EXCHANGE_NAME)
                || (exchangeName.equals(ExchangeDefaults.FANOUT_EXCHANGE_NAME))
                || (exchangeName.equals(ExchangeDefaults.TOPIC_EXCHANGE_NAME));
    }

    public static String getExchangeType(String exchangeName) {
        String ex;
        if (exchangeName == null) {
            ex = "";
        } else {
            ex = exchangeName;
        }
        switch (ex) {
            case "":
            case ExchangeDefaults.DIRECT_EXCHANGE_NAME:
                return ExchangeDefaults.DIRECT_EXCHANGE_CLASS;
            case ExchangeDefaults.FANOUT_EXCHANGE_NAME:
                return ExchangeDefaults.FANOUT_EXCHANGE_CLASS;
            case ExchangeDefaults.TOPIC_EXCHANGE_NAME:
                return ExchangeDefaults.TOPIC_EXCHANGE_CLASS;
            default:
                return "";
        }
    }

    public static String formatExchangeName(String s) {
        return s.replaceAll("\r", "").
                replaceAll("\n", "").trim();
    }

    public static boolean isDefaultExchange(final String exchangeName) {
        return exchangeName == null || AMQShortString.EMPTY_STRING.toString().equals(exchangeName);
    }

}
