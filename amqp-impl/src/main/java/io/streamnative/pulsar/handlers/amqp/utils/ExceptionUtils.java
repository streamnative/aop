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

import io.streamnative.pulsar.handlers.amqp.common.exception.AoPException;
import org.apache.qpid.server.protocol.ErrorCodes;

public class ExceptionUtils {

    public static AoPException getAoPException(Throwable throwable, String msg,
                                               boolean closeChannel, boolean closeConnection) {
        if (throwable instanceof AoPException) {
            return (AoPException) throwable;
        } else {
            return new AoPException(ErrorCodes.INTERNAL_ERROR, msg, closeChannel, closeConnection);
        }
    }

}
