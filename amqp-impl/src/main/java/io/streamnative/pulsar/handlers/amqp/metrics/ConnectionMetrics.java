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
package io.streamnative.pulsar.handlers.amqp.metrics;

import io.prometheus.client.Gauge;
import lombok.Getter;

/**
 * This class is used to manage AoP connection metrics.
 */
public interface ConnectionMetrics {

    String getVhost();

    void inc();

    void dec();

    static ConnectionMetrics create(boolean enableMetrics, String vhost) {
        if (enableMetrics) {
            return new ConnectionMetricsImpl(vhost);
        }
        return new ConnectionMetricsDisable();
    }

    class ConnectionMetricsImpl implements ConnectionMetrics {

        @Getter
        private final String vhost;

        static final Gauge CONNECTION = Gauge.build()
                .name("amqp_connection_counter")
                .labelNames("vhost").help("Amqp connection count.").register();

        public ConnectionMetricsImpl(String vhost) {
            this.vhost = vhost;
        }

        public void inc() {
            CONNECTION.labels(vhost).inc();
        }

        public void dec() {
            CONNECTION.labels(vhost).dec();
        }

    }

    class ConnectionMetricsDisable implements ConnectionMetrics {
        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public void inc() {

        }

        @Override
        public void dec() {

        }

    }

}
