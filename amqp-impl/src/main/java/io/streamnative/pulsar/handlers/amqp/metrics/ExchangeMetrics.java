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

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import lombok.Getter;

/**
 * This class is used to manage AoP exchange metrics.
 */
public interface ExchangeMetrics {

    String getVhost();

    String getExchangeName();

    void writeInc();

    void writeFailed();

    Histogram.Timer startWrite();

    void finishWrite(Histogram.Timer timer);

    void readInc();

    void readFailed();

    Histogram.Timer startRead();

    void finishRead(Histogram.Timer timer);

    void ackInc();

    void routeInc();

    void routeFailedInc();

    Histogram.Timer startRoute();

    void finishRoute(Histogram.Timer timer);

    class ExchangeMetricsImpl implements ExchangeMetrics {
        @Getter
        private final String vhost;
        @Getter
        private final String exchangeName;
        private static final String[] LABELS = {"vhost", "exchange_name"};
        private final String[] labelValues;

        static final Counter WRITE_COUNTER = Counter.build()
                .name("exchange_write_counter")
                .labelNames(LABELS)
                .help("Exchange write counter.").register();

        static final Counter WRITE_FAILED_COUNTER = Counter.build()
                .name("exchange_write_failed_counter")
                .labelNames(LABELS)
                .help("Exchange write failed counter.").register();

        static final Histogram WRITE_LATENCY = Histogram.build()
                .name("exchange_write_latency_seconds")
                .labelNames(LABELS)
                .help("Exchange write latency in seconds.").register();

        static final Counter READ_COUNTER = Counter.build()
                .name("exchange_read_counter")
                .labelNames(LABELS)
                .help("Exchange read counter.").register();

        static final Counter READ_FAILED_COUNTER = Counter.build()
                .name("exchange_read_failed_counter")
                .labelNames(LABELS)
                .help("Exchange read failed counter.").register();

        static final Histogram READ_LATENCY = Histogram.build()
                .name("exchange_read_latency_seconds")
                .labelNames(LABELS)
                .help("Exchange read latency in seconds.").register();

        static final Counter ACK_COUNTER = Counter.build()
                .name("exchange_ack_counter")
                .labelNames(LABELS)
                .help("Exchange ack counter.").register();

        static final Counter ROUTE_COUNTER = Counter.build()
                .name("exchange_route_counter")
                .labelNames(LABELS)
                .help("Exchange route counter").register();

        static final Counter ROUTE_FAILED_COUNTER = Counter.build()
                .name("exchange_route_failed_counter")
                .labelNames(LABELS)
                .help("Exchange route counter").register();

        static final Histogram ROUTE_LATENCY = Histogram.build()
                .name("exchange_route_latency")
                .labelNames(LABELS)
                .help("Exchange route latency").register();

        public ExchangeMetricsImpl(String vhost, String exchangeName) {
            this.vhost = vhost;
            this.exchangeName = exchangeName;
            this.labelValues = new String[]{vhost, exchangeName};

            WRITE_COUNTER.labels(labelValues).inc(0);
            WRITE_FAILED_COUNTER.labels(labelValues).inc(0);
            READ_COUNTER.labels(labelValues).inc(0);
            READ_FAILED_COUNTER.labels(labelValues).inc(0);
            ACK_COUNTER.labels(labelValues).inc(0);
            ROUTE_COUNTER.labels(labelValues).inc(0);
            ROUTE_FAILED_COUNTER.labels(labelValues).inc(0);
        }

        public void writeInc() {
            WRITE_COUNTER.labels(labelValues).inc();
        }

        public void writeFailed() {
            WRITE_FAILED_COUNTER.labels(labelValues).inc();
        }

        public Histogram.Timer startWrite() {
            return WRITE_LATENCY.labels(labelValues).startTimer();
        }

        @Override
        public void finishWrite(Histogram.Timer timer) {
            timer.observeDuration();
        }

        public void readInc() {
            READ_COUNTER.labels(labelValues).inc();
        }

        public void readFailed() {
            READ_FAILED_COUNTER.labels(labelValues).inc();
        }

        public Histogram.Timer startRead() {
            return READ_LATENCY.labels(labelValues).startTimer();
        }

        @Override
        public void finishRead(Histogram.Timer timer) {
            timer.observeDuration();
        }

        public void ackInc() {
            ACK_COUNTER.labels(labelValues).inc();
        }

        public void routeInc() {
            ROUTE_COUNTER.labels(labelValues).inc();
        }

        public void routeFailedInc() {
            ROUTE_FAILED_COUNTER.labels(labelValues).inc();
        }

        public Histogram.Timer startRoute() {
            return ROUTE_LATENCY.labels(labelValues).startTimer();
        }

        @Override
        public void finishRoute(Histogram.Timer timer) {
            timer.observeDuration();
        }

    }

    class ExchangeMetricsDisable implements ExchangeMetrics {

        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public String getExchangeName() {
            return null;
        }

        @Override
        public void writeInc() {

        }

        @Override
        public void writeFailed() {

        }

        @Override
        public Histogram.Timer startWrite() {
            return null;
        }

        @Override
        public void finishWrite(Histogram.Timer timer) {

        }

        @Override
        public void readInc() {

        }

        @Override
        public void readFailed() {

        }

        @Override
        public Histogram.Timer startRead() {
            return null;
        }

        @Override
        public void finishRead(Histogram.Timer timer) {

        }

        @Override
        public void ackInc() {

        }

        @Override
        public void routeInc() {

        }

        @Override
        public void routeFailedInc() {

        }

        @Override
        public Histogram.Timer startRoute() {
            return null;
        }

        @Override
        public void finishRoute(Histogram.Timer timer) {

        }

    }

    static ExchangeMetrics create(boolean enableMetrics, String vhost, String exchangeName) {
        if (enableMetrics) {
            return new ExchangeMetricsImpl(vhost, exchangeName);
        }
        return new ExchangeMetricsDisable();
    }

}
