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
package io.streamnative.pulsar.handlers.amqp.proxy;

import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Configuration for AMQP redirect service.
 */
@Getter
@Setter
public class ProxyConfiguration extends AmqpServiceConfiguration {

    @Category
    private static final String CATEGORY_SERVER = "Server";
    @Category
    private static final String CATEGORY_BROKER_DISCOVERY = "Broker Discovery";
    @Category
    private static final String CATEGORY_HTTP = "HTTP";
    @Category
    private static final String CATEGORY_RATE_LIMITING = "RateLimiting";
    @Category(
            description = "the settings are for configuring how proxies authenticates with Pulsar brokers"
    )
    private static final String CATEGORY_CLIENT_AUTHENTICATION = "Broker Client Authorization";

    /** --------- Server --------- **/
    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "The port for serving binary protobuf request"
    )
    private Optional<Integer> servicePort = Optional.ofNullable(6660);

    @FieldContext(
            category = CATEGORY_SERVER,
            doc = "The port for serving http requests"
    )
    private Optional<Integer> webServicePort = Optional.ofNullable(8090);

    /** --------- Broker Discovery --------- **/
    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            doc = "The ZooKeeper quorum connection string (as a comma-separated list)"
    )
    private String zookeeperServers = "localhost:2181";

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            doc = "ZooKeeper session timeout (in milliseconds)"
    )
    private int zookeeperSessionTimeoutMs = 1000 * 5;

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            doc = "The service url points to the broker cluster"
    )
    private String brokerServiceURL = "pulsar://localhost:6650";

    @FieldContext(
            category = CATEGORY_BROKER_DISCOVERY,
            doc = "The tls service url points to the broker cluster"
    )
    private String brokerServiceURLTLS;

    /** --------- HTTP --------- **/
    @FieldContext(
            minValue = 1,
            category = CATEGORY_HTTP,
            doc = "Http output buffer size.\n\n"
                    + "The amount of data that will be buffered for http requests "
                    + "before it is flushed to the channel. A larger buffer size may "
                    + "result in higher http throughput though it may take longer for "
                    + "the client to see data. If using HTTP streaming via the reverse "
                    + "proxy, this should be set to the minimum value, 1, so that clients "
                    + "see the data as soon as possible."
    )
    private int httpOutputBufferSize = 32*1024;

    @FieldContext(
            minValue = 1,
            category = CATEGORY_HTTP,
            doc = "Number of threads to use for HTTP requests processing"
    )
    private int httpNumThreads = Math.max(8, 2 * Runtime.getRuntime().availableProcessors());

    @FieldContext(
            category = CATEGORY_RATE_LIMITING,
            doc = "Max concurrent lookup requests. The proxy will reject requests beyond that"
    )
    private int maxConcurrentLookupRequests = 50000;

    @FieldContext(
            category = CATEGORY_CLIENT_AUTHENTICATION,
            doc = "Whether TLS is enabled when communicating with Pulsar brokers"
    )
    private boolean tlsEnabledWithBroker = false;


}
