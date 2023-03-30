/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.streamnative.pulsar.handlers.amqp.admin;

import io.streamnative.pulsar.handlers.amqp.admin.impl.BaseResources;
import io.streamnative.pulsar.handlers.amqp.admin.impl.ExchangeBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Vhost endpoints.
 */
@Slf4j
@Path("/vhosts")
@Produces(MediaType.APPLICATION_JSON)
public class Vhosts extends ExchangeBase {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getVhostListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get vhost list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @PUT
    @Path("/{vhost}")
    public void declareVhost(@Suspended final AsyncResponse response,
                             @PathParam("vhost") String vhost) throws PulsarServerException {
        PulsarAdmin pulsarAdmin = pulsarAdmin();
        pulsarAdmin.namespaces()
                .createNamespaceAsync(tenant + "/" + vhost)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare vhost {}/{} ", tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
