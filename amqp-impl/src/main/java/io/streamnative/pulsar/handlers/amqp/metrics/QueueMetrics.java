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
 * This class is used to manage AoP queue metrics.
 */
public interface QueueMetrics {

    String getVhost();

    String getQueueName();

    void writeInc();

    void writeFailed();

    Histogram.Timer startWrite();

    void finishWrite(Histogram.Timer timer);

    void dispatchInc();

    void readFailed();

    Histogram.Timer startRead();

    void finishRead(Histogram.Timer timer);

    void ackInc();

    class QueueMetricsImpl implements QueueMetrics {

        @Getter
        private final String vhost;
        @Getter
        private final String queueName;
        private static final String[] LABELS = {"vhost", "queue_name"};
        private final String[] labelValues;

        static final Counter WRITE_COUNTER = Counter.build()
                .name("queue_write_counter")
                .labelNames(LABELS)
                .help("Queue write counter.").register();

        static final Counter WRITE_FAILED_COUNTER = Counter.build()
                .name("queue_write_failed_counter")
                .labelNames(LABELS)
                .help("Queue write failed counter.").register();

        static final Histogram WRITE_LATENCY = Histogram.build()
                .name("queue_write_latency_seconds")
                .labelNames(LABELS)
                .help("Queue write latency in seconds.").register();

        static final Counter DISPATCH_COUNTER = Counter.build()
                .name("queue_dispatch_counter")
                .labelNames(LABELS)
                .help("Queue read counter.").register();

        static final Counter READ_FAILED_COUNTER = Counter.build()
                .name("queue_read_failed_counter")
                .labelNames(LABELS)
                .help("Queue read failed counter.").register();

        static final Histogram READ_LATENCY = Histogram.build()
                .name("queue_read_latency_seconds")
                .labelNames(LABELS)
                .help("Queue read latency in seconds.").register();

        static final Counter ACK_COUNTER = Counter.build()
                .name("queue_ack_counter")
                .labelNames(LABELS)
                .help("Queue ack counter.").register();

        public QueueMetricsImpl(String vhost, String queueName) {
            this.vhost = vhost;
            this.queueName = queueName;
            this.labelValues = new String[]{vhost, queueName};

            WRITE_COUNTER.labels(labelValues).inc(0);
            WRITE_FAILED_COUNTER.labels(labelValues).inc(0);
            DISPATCH_COUNTER.labels(labelValues).inc(0);
            READ_FAILED_COUNTER.labels(labelValues).inc(0);
            ACK_COUNTER.labels(labelValues).inc(0);
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

        public void dispatchInc() {
            DISPATCH_COUNTER.labels(labelValues).inc();
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

    }

    class QueueMetricsDisable implements QueueMetrics {
        @Override
        public String getVhost() {
            return null;
        }

        @Override
        public String getQueueName() {
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
        public void dispatchInc() {

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

    }

    static QueueMetrics create(boolean enableMetrics, String vhost, String queueName) {
        if (enableMetrics) {
            return new QueueMetricsImpl(vhost, queueName);
        }
        return new QueueMetricsDisable();
    }

}
