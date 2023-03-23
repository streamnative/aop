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
package io.streamnative.pulsar.handlers.amqp.admin;

import io.streamnative.pulsar.handlers.amqp.admin.impl.ExchangeBase;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

/**
 * Vhost endpoints.
 */
@Slf4j
@Path("/vhosts")
@Produces(MediaType.APPLICATION_JSON)
public class Vhosts extends ExchangeBase {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getAllVhostListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get vhost list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
