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

import static org.awaitility.Awaitility.await;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.rabbitmq.client.LongString;
import com.rabbitmq.client.SaslConfig;
import com.rabbitmq.client.SaslMechanism;
import com.rabbitmq.client.impl.CredentialsProvider;
import com.rabbitmq.client.impl.LongStringHelper;
import io.jsonwebtoken.SignatureAlgorithm;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.crypto.SecretKey;
import org.apache.pulsar.broker.authentication.utils.AuthTokenUtils;
import org.testng.annotations.Test;

/**
 * TokenSaslConfigTest tests {@link io.streamnative.rabbitmq.authentication.TokenSaslConfig}.
 */
public class TokenSaslConfigTest {
    private final SecretKey secretKey = AuthTokenUtils.createSecretKey(SignatureAlgorithm.HS256);

    @Test
    public void testTokenSaslConfig() {
        SaslConfig saslConfig = new TokenSaslConfig();
        SaslMechanism saslMechanism = saslConfig.getSaslMechanism(new String[]{"PLAIN", "token"});
        assertEquals(saslMechanism.getName(), "token");

        String token = "your token";
        LongString data = saslMechanism.handleChallenge(null, null, token);
        assertEquals(data, LongStringHelper.asLongString(token));
    }

    @Test
    public void testUnSupportMechanism() {
        SaslConfig saslConfig = new TokenSaslConfig();
        assertThrows(MechanismException.class, () -> saslConfig.getSaslMechanism(new String[]{"PLAIN"}));
    }

    @Test
    public void testGetTimeBeforeExpiration() {
        String token = AuthTokenUtils.createToken(secretKey, "client",
                Optional.of(Date.from(Instant.now().plusSeconds(5))));
        CredentialsProvider c = new TokenCredentialsProvider(() -> token);
        Duration duration = c.getTimeBeforeExpiration();
        assertNotNull(duration);
        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertTrue(c.getTimeBeforeExpiration().getSeconds() >= 0);
        });
    }

    @Test
    public void testNullGetTimeBeforeExpiration() {
        String token = AuthTokenUtils.createToken(secretKey, "client", Optional.empty());
        CredentialsProvider c = new TokenCredentialsProvider(() -> token);
        assertNull(c.getTimeBeforeExpiration());
    }
}
