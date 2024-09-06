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
package io.streamnative.pulsar.handlers.amqp.authentication;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.Crypt;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.client.api.url.URL;

public class AuthenticationProviderBasic implements AuthenticationProvider {
    private static final String HTTP_HEADER_NAME = "Authorization";
    private Map<String, String> users;

    AuthenticationMetrics metrics;

    private enum ErrorCode {
        UNKNOWN,
        EMPTY_AUTH_DATA,
        INVALID_HEADER,
        INVALID_AUTH_DATA,
        INVALID_TOKEN,
    }

    @SneakyThrows
    @Override
    public void initialize(ServiceConfiguration config) {
        initialize(Context.builder().config(config).build());
    }

    @SneakyThrows
    @Override
    public void initialize(Context context) throws IOException {
        metrics = new AuthenticationMetrics(context.getOpenTelemetry(),
                getClass().getSimpleName(), getAuthMethodName());
        String basicAuthConf = (String) context.getConfig().getProperty("basicAuthConf");

        byte[] data;
        boolean isFile = basicAuthConf.startsWith("file:");
        if (isFile) {
            data = IOUtils.toByteArray(URL.createURL(basicAuthConf));
        } else if (Base64.isBase64(basicAuthConf)) {
            data = Base64.decodeBase64(basicAuthConf);
        } else {
            throw new IllegalArgumentException("Not support format");
        }

        @Cleanup BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(data),
                StandardCharsets.UTF_8));
        users = new HashMap<>();
        for (String line : reader.lines().toArray(s -> new String[s])) {
            List<String> splitLine = Arrays.asList(line.split(":"));
            if (splitLine.size() != 2) {
                throw new IOException("The format of the password auth conf file is invalid");
            }
            users.put(splitLine.get(0), splitLine.get(1));
        }
    }

    @Override
    public String getAuthMethodName() {
        return "basic";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        AuthParams authParams = new AuthParams(authData);
        String userId = authParams.getUserId();
        String password = authParams.getPassword();
        String msg = "Unknown user or invalid password";

        ErrorCode errorCode = ErrorCode.UNKNOWN;
        try {
            if (users.get(userId) == null) {
                errorCode = ErrorCode.EMPTY_AUTH_DATA;
                throw new AuthenticationException(msg);
            }

            String encryptedPassword = users.get(userId);

            // For md5 algorithm
            if ((users.get(userId).startsWith("$apr1"))) {
                List<String> splitEncryptedPassword = Arrays.asList(encryptedPassword.split("\\$"));
                if (splitEncryptedPassword.size() != 4 || !encryptedPassword
                        .equals(Md5Crypt.apr1Crypt(password.getBytes(StandardCharsets.UTF_8),
                                splitEncryptedPassword.get(2)))) {
                    errorCode = ErrorCode.INVALID_TOKEN;
                    throw new AuthenticationException(msg);
                }
                // For crypt algorithm
            } else if (!encryptedPassword.equals(Crypt.crypt(password.getBytes(StandardCharsets.UTF_8),
                    encryptedPassword.substring(0, 2)))) {
                errorCode = ErrorCode.INVALID_TOKEN;
                throw new AuthenticationException(msg);
            }
        } catch (AuthenticationException exception) {
            incrementFailureMetric(errorCode);
            throw exception;
        }
        metrics.recordSuccess();
        return userId;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    private class AuthParams {
        private final String userId;
        private final String password;

        public AuthParams(AuthenticationDataSource authData) throws AuthenticationException {
            String authParams;
            if (authData.hasDataFromCommand()) {
                authParams = authData.getCommandData();
            } else if (authData.hasDataFromHttp()) {
                String rawAuthToken = authData.getHttpHeader(HTTP_HEADER_NAME);
                // parsing and validation
                if (StringUtils.isBlank(rawAuthToken) || !rawAuthToken.toUpperCase().startsWith("BASIC ")) {
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Authentication token has to be started with \"Basic \"");
                }
                String[] splitRawAuthToken = rawAuthToken.split(" ");
                if (splitRawAuthToken.length != 2) {
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Base64 encoded token is not found");
                }

                try {
                    authParams = new String(java.util.Base64.getDecoder().decode(splitRawAuthToken[1]),
                            StandardCharsets.UTF_8);
                } catch (Exception e) {
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Base64 decoding is failure: " + e.getMessage());
                }
            } else {
                incrementFailureMetric(ErrorCode.EMPTY_AUTH_DATA);
                throw new AuthenticationException("Authentication data source does not have data");
            }

            String[] parsedAuthParams = authParams.split(":");
            if (parsedAuthParams.length != 2) {
                incrementFailureMetric(ErrorCode.INVALID_AUTH_DATA);
                throw new AuthenticationException("Base64 decoded params are invalid");
            }

            userId = parsedAuthParams[0];
            password = parsedAuthParams[1];
        }

        public String getUserId() {
            return userId;
        }

        public String getPassword() {
            return password;
        }
    }

    @Override
    public void incrementFailureMetric(Enum<?> errorCode) {
        metrics.recordFailure(errorCode);
    }

}
