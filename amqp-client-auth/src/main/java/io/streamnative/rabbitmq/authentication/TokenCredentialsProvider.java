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

import com.rabbitmq.client.impl.CredentialsProvider;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Header;
import io.jsonwebtoken.Jwt;
import io.jsonwebtoken.Jwts;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * TokenCredentialsProvider provides a token credential provider that supports refresh.
 */
@Slf4j
public class TokenCredentialsProvider implements CredentialsProvider {
    private final Supplier<String> tokenSupplier;
    private String token;
    private Jwt<Header, Claims> jwt;

    public TokenCredentialsProvider(Supplier<String> tokenSupplier) {
        load(tokenSupplier);
        this.tokenSupplier = tokenSupplier;
    }

    private void load(Supplier<String> tokenSupplier) {
        String token = tokenSupplier.get();
        int i = token.lastIndexOf('.');
        String withoutSignature = token.substring(0, i + 1);
        this.jwt = Jwts.parserBuilder().build().parseClaimsJwt(withoutSignature);
        this.token = token;
    }

    @Override
    public String getUsername() {
        return null;
    }

    @Override
    public String getPassword() {
        return token;
    }

    @Override
    public Duration getTimeBeforeExpiration() {
        Date expiration = jwt.getBody().getExpiration();
        if (expiration == null) {
            return null;
        }
        return Duration.between(expiration.toInstant(), Instant.now());
    }

    @Override
    public void refresh() {
        try {
            load(tokenSupplier);
        } catch (Exception e) {
            log.error("Failed to refresh token", e);
        }
    }
}
