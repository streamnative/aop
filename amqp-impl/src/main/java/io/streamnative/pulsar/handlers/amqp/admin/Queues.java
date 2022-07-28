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

import io.streamnative.pulsar.handlers.amqp.admin.impl.QueueBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.DELETE;
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


@Slf4j
@Path("/queues")
@Produces(MediaType.APPLICATION_JSON)
public class Queues extends QueueBase {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getQueueListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get queue list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}")
    public void getListByVhost(@Suspended final AsyncResponse response,
                               @PathParam("vhost") String vhost) {
        getQueueListByVhostAsync(vhost)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get queue list in vhost {}", vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{queue}")
    public void getQueue(@Suspended final AsyncResponse response,
                            @PathParam("vhost") String vhost,
                            @PathParam("queue") String queue) {
        getQueueBeanAsync(vhost, queue)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get queue {} in vhost {}", queue, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @PUT
    @Path("/{vhost}/{queue}")
    public void declareQueue(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("queue") String queue,
                                QueueDeclareParams params) {
        declareQueueAsync(vhost, queue, params)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    log.error("Failed to declare queue {} {} in vhost {}", queue, tenant, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/{vhost}/{queue}")
    public void deleteQueue(@Suspended final AsyncResponse response,
                            @PathParam("vhost") String vhost,
                            @PathParam("queue") String queue,
                            @QueryParam("if-unused") boolean ifUnused,
                            @QueryParam("if-empty") boolean ifEmpty) {
        deleteQueueAsync(vhost, queue, ifUnused, ifEmpty)
                .thenAccept(__ -> {
                    log.info("Success delete queue {} in vhost {}, if-unused is {}, if-empty is {}",
                            queue, vhost, ifUnused, ifEmpty);
                    response.resume(Response.noContent().build());
                })
                .exceptionally(t -> {
                    log.error("Failed to delete queue {} in vhost {}", queue, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
