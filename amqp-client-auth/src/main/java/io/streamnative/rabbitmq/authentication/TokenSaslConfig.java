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
package io.streamnative.rabbitmq.authentication;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.SaslMechanism;
import com.rabbitmq.client.impl.LongStringHelper;
import java.util.Arrays;
import lombok.SneakyThrows;
import org.apache.commons.lang3.ArrayUtils;

/**
 * TokenSaslConfig provides a sasl config for OAuth2 or JWT.
 */
public class TokenSaslConfig implements SaslConfig {
    private static final String mechanism = "token";

    @SneakyThrows
    @Override
    public SaslMechanism getSaslMechanism(String[] mechanisms) {
        if (ArrayUtils.isEmpty(mechanisms) || Arrays.stream(mechanisms).noneMatch(n -> n.equals(mechanism))) {
            throw new MechanismException(String.format("%s mechanism is not supported, available mechanisms: %s",
                    mechanism, Arrays.toString(mechanisms)));
        }

        return new SaslMechanism() {
            @Override
            public String getName() {
                return mechanism;
            }

            @Override
            public LongString handleChallenge(LongString challenge, String username, String password) {
                return LongStringHelper.asLongString(password);
            }
        };
    }
}
