

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

package com.rabbitmq.client.test;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.utility.Utility;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Convenience class: an implementation of {@link Consumer} with
 * straightforward blocking semantics. It is meant to be using in
 * tests.
 * Deprecated in favor of {@link DefaultConsumer} (see below for background).
 * Will be removed in next major release.
 * The general pattern for using QueueingConsumer is as follows:
 * <pre>
 * // Create connection and channel.
 * {@link ConnectionFactory} factory = new ConnectionFactory();
 * Connection conn = factory.newConnection();
 * {@link Channel} ch1 = conn.createChannel();
 *
 * // Create the QueueingConsumer and have it consume from the queue
 * QueueingConsumer consumer = new {@link QueueingConsumer#QueueingConsumer(Channel) QueueingConsumer}(ch1);
 * ch1.{@link Channel#basicConsume basicConsume}(queueName, false, consumer);
 *
 * // Process deliveries
 * while (/* some condition * /) {
 *     {@link Delivery} delivery = consumer.{@link QueueingConsumer#nextDelivery nextDelivery}();
 *     // process delivery
 *     ch1.{@link Channel#basicAck basicAck}(delivery.
 *     {@link Delivery#getEnvelope getEnvelope}().{@link Envelope#getDeliveryTag getDeliveryTag}(), false);
 * }
 * </pre>
 *
 *
 * <p>For a more complete example, see LogTail in the <code>test/src/com/rabbitmq/examples</code>
 * directory of the source distribution.</p>
 *
 * <h3>Historical Perspective</h3>
 *<code>QueueingConsumer</code> was introduced to allow
 * applications to overcome a limitation in the way <code>Connection</code>
 * managed threads and consumer dispatching. When <code>QueueingConsumer</code>
 * was introduced, callbacks to <code>Consumers</code> were made on the
 * <code>Connection's</code> thread. This had two main drawbacks. Firstly, the
 * <code>Consumer</code> could stall the processing of all
 * <code>Channels</code> on the <code>Connection</code>. Secondly, if a
 * <code>Consumer</code> made a recursive synchronous call into its
 * <code>Channel</code> the client would deadlock.
 * <code>QueueingConsumer</code> provided client code with an easy way to
 * obviate this problem by queueing incoming messages and processing them on
 * a separate, application-managed thread.
 * The threading behaviour of <code>Connection</code> and <code>Channel</code>
 * has been changed so that each <code>Channel</code> uses a distinct thread
 * for dispatching to <code>Consumers</code>. This prevents
 * <code>Consumers</code> on one <code>Channel</code> holding up
 * <code>Consumers</code> on another and it also prevents recursive calls from
 * deadlocking the client.
 * As such, it is now safe to implement <code>Consumer</code> directly or
 * to extend <code>DefaultConsumer</code> and <code>QueueingConsumer</code>
 * is a lot less relevant.
 *
 */
public class QueueingConsumer extends DefaultConsumer {
    private final BlockingQueue<Delivery> queue;

    // When this is non-null the queue is in shutdown mode and nextDelivery should
    // throw a shutdown signal exception.
    private volatile ShutdownSignalException shutdown;
    private volatile ConsumerCancelledException cancelled;

    // Marker object used to signal the queue is in shutdown mode.
    // It is only there to wake up consumers. The canonical representation
    // of shutting down is the presence of _shutdown.
    // Invariant: This is never on _queue unless _shutdown != null.
    private static final Delivery POISON = new Delivery(null, null, null);

    public QueueingConsumer(Channel ch) {
        this(ch, new LinkedBlockingQueue<Delivery>());
    }

    public QueueingConsumer(Channel ch, BlockingQueue<Delivery> q) {
        super(ch);
        this.queue = q;
    }

    @Override
    public void handleShutdownSignal(String consumerTag,
                                     ShutdownSignalException sig) {
        shutdown = sig;
        queue.add(POISON);
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {
        cancelled = new ConsumerCancelledException();
        queue.add(POISON);
    }

    @Override
    public void handleDelivery(String consumerTag,
                               Envelope envelope,
                               BasicProperties properties,
                               byte[] body)
            throws IOException {
        checkShutdown();
        this.queue.add(new Delivery(envelope, properties, body));
    }

    /**
     * Encapsulates an arbitrary message - simple "bean" holder structure.
     */
    public static class Delivery {
        private final Envelope envelope;
        private final BasicProperties properties;
        private final byte[] body;

        public Delivery(Envelope envelope, BasicProperties properties, byte[] body) {
            this.envelope = envelope;
            this.properties = properties;
            this.body = body;
        }

        /**
         * Retrieve the message envelope.
         * @return the message envelope
         */
        public Envelope getEnvelope() {
            return envelope;
        }

        /**
         * Retrieve the message properties.
         * @return the message properties
         */
        public BasicProperties getProperties() {
            return properties;
        }

        /**
         * Retrieve the message body.
         * @return the message body
         */
        public byte[] getBody() {
            return body;
        }
    }

    /**
     * Check if we are in shutdown mode and if so throw an exception.
     */
    private void checkShutdown() {
        if (shutdown != null) {
            throw Utility.fixStackTrace(shutdown);
        }
    }

    /**
     * If delivery is not POISON nor null, return it.
     * <p/>
     * If delivery, _shutdown and _cancelled are all null, return null.
     * <p/>
     * If delivery is POISON re-insert POISON into the queue and
     * throw an exception if POISONed for no reason.
     * <p/>
     * Otherwise, if we are in shutdown mode or cancelled,
     * throw a corresponding exception.
     */
    private Delivery handle(Delivery delivery) {
        if (delivery == POISON
                ||
                delivery == null && (shutdown != null || cancelled != null)) {
            if (delivery == POISON) {
                queue.add(POISON);
                if (shutdown == null && cancelled == null) {
                    throw new IllegalStateException(
                            "POISON in queue, but null _shutdown and null _cancelled. "
                                    +
                                    "This should never happen, please report as a BUG");
                }
            }
            if (null != shutdown) {
                throw Utility.fixStackTrace(shutdown);
            }
            if (null != cancelled) {
                throw Utility.fixStackTrace(cancelled);
            }
        }
        return delivery;
    }

    /**
     * Main application-side API: wait for the next message delivery and return it.
     * @return the next message
     * @throws InterruptedException if an interrupt is received while waiting
     * @throws ShutdownSignalException if the connection is shut down while waiting
     * @throws ConsumerCancelledException if this consumer is cancelled while waiting
     */
    public Delivery nextDelivery()
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return handle(queue.take());
    }

    /**
     * Main application-side API: wait for the next message delivery and return it.
     * @param timeout timeout in millisecond
     * @return the next message or null if timed out
     * @throws InterruptedException if an interrupt is received while waiting
     * @throws ShutdownSignalException if the connection is shut down while waiting
     * @throws ConsumerCancelledException if this consumer is cancelled while waiting
     */
    public Delivery nextDelivery(long timeout)
            throws InterruptedException, ShutdownSignalException, ConsumerCancelledException {
        return handle(queue.poll(timeout, TimeUnit.MILLISECONDS));
    }
}
