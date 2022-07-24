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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to manage AoP metrics.
 */
public interface AmqpMetrics {

    void connectionInc(String vhost);

    void connectionDec(String vhost);

    ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName);

    void deleteExchangeMetrics(String vhost, String exchangeName);

    QueueMetrics addQueueMetrics(String vhost, String queueName);

    void deleteQueueMetrics(String vhost, String queueName);

    class AmqpMetricsImpl implements AmqpMetrics {

        private final boolean enableMetrics;
        private final Map<String, ConnectionMetrics> connectionMetricsMap = new ConcurrentHashMap<>();
        private final Map<String, Map<String, ExchangeMetrics>> exchangeMetricsMap = new ConcurrentHashMap<>();
        private final Map<String, Map<String, QueueMetrics>> queueMetricsMap = new ConcurrentHashMap<>();

        static Gauge vhostGauge = Gauge.build()
                .name("vhost_count")
                .help("Vhost count").register();

        static Gauge exchangeGauge = Gauge.build()
                .name("exchange_count")
                .labelNames("vhost")
                .help("Exchange count").register();

        static Gauge queueGauge = Gauge.build()
                .name("queue_count")
                .labelNames("vhost")
                .help("Queue count").register();

        public AmqpMetricsImpl(boolean enableMetrics) {
            this.enableMetrics = enableMetrics;
        }

        public void connectionInc(String vhost) {
            connectionMetricsMap.computeIfAbsent(vhost, k -> ConnectionMetrics.create(enableMetrics, vhost)).inc();
            updateVhostGauge();
        }

        public void connectionDec(String vhost) {
            connectionMetricsMap.computeIfPresent(vhost, (k, metrics) -> {
                metrics.dec();
                return metrics;
            });
            updateVhostGauge();
        }

        public ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName) {
            ExchangeMetrics exchangeMetrics = ExchangeMetrics.create(this.enableMetrics, vhost, exchangeName);
            exchangeMetricsMap.computeIfAbsent(exchangeMetrics.getVhost(), k -> new ConcurrentHashMap<>())
                    .putIfAbsent(exchangeMetrics.getExchangeName(), exchangeMetrics);
            updateExchangeGauge(vhost);
            return exchangeMetrics;
        }

        public void deleteExchangeMetrics(String vhost, String exchangeName) {
            exchangeMetricsMap.computeIfPresent(vhost, (key, map) -> {
                map.remove(exchangeName);
                return map;
            });
            updateExchangeGauge(vhost);
        }

        public QueueMetrics addQueueMetrics(String vhost, String queueName) {
            QueueMetrics queueMetrics = QueueMetrics.create(this.enableMetrics, vhost, queueName);
            queueMetricsMap.computeIfAbsent(queueMetrics.getVhost(), k -> new ConcurrentHashMap<>())
                    .putIfAbsent(queueMetrics.getQueueName(), queueMetrics);
            updateQueueGauge(vhost);
            return queueMetrics;
        }

        public void deleteQueueMetrics(String vhost, String queueName) {
            queueMetricsMap.computeIfPresent(vhost, (key, map) -> {
                map.remove(queueName);
                return map;
            });
            updateQueueGauge(vhost);
        }

        private void updateVhostGauge() {
            vhostGauge.set(connectionMetricsMap.size());
        }

        private void updateExchangeGauge(String vhost) {
            exchangeGauge.labels(vhost)
                    .set(exchangeMetricsMap.getOrDefault(vhost, new HashMap<>(0)).size());
        }

        private void updateQueueGauge(String vhost) {
            queueGauge.labels(vhost)
                    .set(queueMetricsMap.getOrDefault(vhost, new HashMap<>(0)).size());
        }

    }

    class AmqpMetricsDisable implements AmqpMetrics {
        @Override
        public void connectionInc(String vhost) {

        }

        @Override
        public void connectionDec(String vhost) {

        }

        @Override
        public ExchangeMetrics addExchangeMetrics(String vhost, String exchangeName) {
            return ExchangeMetrics.create(false, vhost, exchangeName);
        }

        @Override
        public void deleteExchangeMetrics(String vhost, String exchangeName) {

        }

        @Override
        public QueueMetrics addQueueMetrics(String vhost, String queueName) {
            return QueueMetrics.create(false, vhost, queueName);
        }

        @Override
        public void deleteQueueMetrics(String vhost, String queueName) {

        }

    }

    static AmqpMetrics create(boolean enableMetrics) {
        if (enableMetrics) {
            return new AmqpMetricsImpl(enableMetrics);
        }
        return new AmqpMetricsDisable();
    }

}
