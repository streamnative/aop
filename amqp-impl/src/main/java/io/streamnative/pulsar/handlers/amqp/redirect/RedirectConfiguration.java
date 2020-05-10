package io.streamnative.pulsar.handlers.amqp.redirect;

import io.streamnative.pulsar.handlers.amqp.AmqpServiceConfiguration;
import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

import java.util.Optional;

/**
 * Configuration for AMQP redirect service.
 */
@Getter
@Setter
public class RedirectConfiguration extends AmqpServiceConfiguration {

    @Category
    private static final String CATEGORY_SERVER = "Server";
    @Category
    private static final String CATEGORY_BROKER_DISCOVERY = "Broker Discovery";
    @Category
    private static final String CATEGORY_HTTP = "HTTP";

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

}
