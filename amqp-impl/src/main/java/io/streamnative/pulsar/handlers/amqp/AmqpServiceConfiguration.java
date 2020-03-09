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
package io.streamnative.pulsar.handlers.amqp;

import lombok.Getter;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.configuration.Category;
import org.apache.pulsar.common.configuration.FieldContext;

/**
 * Amqp on Pulsar service configuration object.
 */
@Getter
@Setter
public class AmqpServiceConfiguration extends ServiceConfiguration {

    @Category
    private static final String CATEGORY_AMQP = "AMQP on Pulsar";

    //
    // --- AMQP on Pulsar Broker configuration ---
    //

    @FieldContext(
            category = CATEGORY_AMQP,
            required = true,
            doc = "Amqp on Pulsar Broker tenant"
    )
    private String amqpTenant = "public";

    @FieldContext(
            category = CATEGORY_AMQP,
            required = true,
            doc = "The tenant used for storing Amqp metadata topics"
    )
    private String amqpMetadataTenant = "public";

    @FieldContext(
            category = CATEGORY_AMQP,
            required = true,
            doc = "The namespace used for storing Amqp metadata topics"
    )
    private String amqpMetadataNamespace = "__amqp";

    @FieldContext(
            category = CATEGORY_AMQP,
            required = true,
            doc = "The namespace used for storing Amqp metadata topics"
    )
    private String amqpListeners = "amqp://127.0.0.1:5672";

}
