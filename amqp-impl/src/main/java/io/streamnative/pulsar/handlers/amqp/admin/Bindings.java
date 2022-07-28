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

import io.streamnative.pulsar.handlers.amqp.admin.impl.BindingBase;
import io.streamnative.pulsar.handlers.amqp.admin.impl.QueueBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
@Path("/bindings")
@Produces(MediaType.APPLICATION_JSON)
public class Bindings extends BindingBase {

    @GET
    @Path("/{vhost}/e/{exchange}/q/{queue}")
    public void getList(@Suspended final AsyncResponse response,
                        @PathParam("vhost") String vhost,
                        @PathParam("exchange") String exchange,
                        @PathParam("queue") String queue) {
        getBindingsAsync(vhost, exchange, queue, null)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get binding list for queue {} and exchange {} in vhost {}",
                            queue, exchange, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @POST
    @Path("/{vhost}/e/{exchange}/q/{queue}")
    public void queueBind(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                @PathParam("queue") String queue,
                                BindingParams params) {
        queueBindAsync(vhost, exchange, queue, params)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    log.error("Failed to bind queue {} to exchange {} with key {} in vhost {}",
                            queue, exchange, params.getRoutingKey(), vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/e/{exchange}/q/{queue}/{props}")
    public void getQueueBinding(@Suspended final AsyncResponse response,
                             @PathParam("vhost") String vhost,
                             @PathParam("exchange") String exchange,
                             @PathParam("queue") String queue,
                             @PathParam("props") String propsKey) {
        getBindingsByPropsKeyAsync(vhost, exchange, queue, propsKey)
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get queue {} to exchange {} with key {} in vhost {}",
                            queue, exchange, propsKey, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/{vhost}/e/{exchange}/q/{queue}/{props}")
    public void queuUnbind(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                @PathParam("queue") String queue,
                                @PathParam("props") String propsKey,
                                QueueDeclareParams params) {
        queueUnbindAsync(vhost, exchange, queue, propsKey)
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    log.error("Failed to unbind queue {} to exchange {} with key {} in vhost {}",
                            queue, exchange, propsKey, vhost, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
