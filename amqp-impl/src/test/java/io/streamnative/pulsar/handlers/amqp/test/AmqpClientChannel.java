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
package io.streamnative.pulsar.handlers.amqp.test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Amqp client channel for receive responses from server.
 */
@Slf4j
public class AmqpClientChannel {

    private final BlockingQueue<Object> responses;

    public AmqpClientChannel() {
        this.responses = new LinkedBlockingQueue<>();
    }

    public void add(Object response) {
        responses.add(response);
    }

    public Object poll(long timeout, TimeUnit unit) {
        try {
            return responses.poll(timeout, unit);
        } catch (InterruptedException e) {
            log.error("Failed to pull from channel", e);
            return null;
        }
    }
}
