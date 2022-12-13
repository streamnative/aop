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
import io.streamnative.pulsar.handlers.amqp.admin.model.ExchangeDeclareParams;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import javax.ws.rs.DELETE;
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
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Exchange endpoints.
 */
@Slf4j
@Path("/exchanges")
@Produces(MediaType.APPLICATION_JSON)
public class Exchanges extends ExchangeBase {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getExchangeListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get exchange list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}")
    public void getListByVhost(@Suspended final AsyncResponse response,
                               @PathParam("vhost") String vhost) {
        getExchangeListByVhostAsync(vhost)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get exchange list by vhost {} for tenant {}", vhost, tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{exchange}")
    public void getExchange(@Suspended final AsyncResponse response,
                            @PathParam("vhost") String vhost,
                            @PathParam("exchange") String exchange) {
        getExchangeBeanAsync(vhost, exchange)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get exchange {} for tenant {} belong to vhost {}",
                            exchange, tenant, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @PUT
    @Path("/{vhost}/{exchange}")
    public void declareExchange(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                ExchangeDeclareParams params,
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                NamespaceName.get("public", vhost), PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> declareExchange(vhost, exchange, params))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare exchange {} for tenant {} belong to vhost {}",
                                exchange, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/{vhost}/{exchange}")
    public void declareExchange(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                @QueryParam("if-unused") boolean ifUnused) {
        deleteExchange(vhost, exchange, ifUnused)
                .thenAccept(__ -> {
                    log.info("Success delete exchange {} in vhost {}, ifUnused is {}", exchange, vhost, ifUnused);
                    response.resume(Response.noContent().build());
                })
                .exceptionally(t -> {
                    log.error("Failed to delete exchange {} for tenant {} belong to vhost {}",
                            exchange, tenant, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
