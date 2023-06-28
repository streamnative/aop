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

import io.streamnative.pulsar.handlers.amqp.admin.impl.QueueBase;
import io.streamnative.pulsar.handlers.amqp.admin.model.BindingParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeclareParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueDeleteParams;
import io.streamnative.pulsar.handlers.amqp.admin.model.QueueUnBindingParams;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentExchange;
import io.streamnative.pulsar.handlers.amqp.impl.PersistentQueue;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Queue endpoints.
 */
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
    @Path("/{vhost}/{queue}/loadQueue")
    public void loadQueue(@Suspended final AsyncResponse response,
                          @PathParam("vhost") String vhost,
                          @PathParam("queue") String queue,
                          @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                getNamespaceName(vhost), PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> loadQueueAsync(vhost, queue))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare queue {} {} in vhost {}", queue, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @PUT
    @Path("/{vhost}/{queue}/startExpirationDetection")
    public void startExpirationDetection(@Suspended final AsyncResponse response,
                                         @PathParam("vhost") String vhost,
                                         @PathParam("queue") String queue,
                                         @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                getNamespaceName(vhost), PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> startExpirationDetection(vhost, queue))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare queue {} {} in vhost {}", queue, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/{vhost}/{queue}/bindings")
    public void getQueueBindings(@Suspended final AsyncResponse response,
                                 @PathParam("vhost") String vhost,
                                 @PathParam("queue") String queue) {
        getQueueBindings(getNamespaceName(vhost), queue)
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
                             QueueDeclareParams params,
                             @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName =
                TopicName.get(TopicDomain.persistent.toString(), namespaceName, PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> declareQueueAsync(namespaceName, queue, params))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to declare queue {} {} in vhost {}", queue, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @POST
    @Path("/{vhost}/e/{exchange}/q/{queue}")
    public void queueBindings(@Suspended final AsyncResponse response,
                              @PathParam("vhost") String vhost,
                              @PathParam("exchange") String exchange,
                              @PathParam("queue") String queue,
                              BindingParams params,
                              @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName =
                TopicName.get(TopicDomain.persistent.toString(), namespaceName,
                        PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> queueBindAsync(namespaceName, exchange, queue, params))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .thenRunAsync(() -> amqpAdmin().queueBindExchange(namespaceName, exchange, queue, params))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to update queue {} {} in vhost {}", queue, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @DELETE
    @Path("/{vhost}/e/{exchange}/q/{queue}/unbind")
    public void queueUnBindings(@Suspended final AsyncResponse response,
                                @PathParam("vhost") String vhost,
                                @PathParam("exchange") String exchange,
                                @PathParam("queue") String queue,
                                QueueUnBindingParams params,
                                @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentExchange.TOPIC_PREFIX + exchange);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> queueUnbindAsync(namespaceName, exchange, queue, params.getProperties_key()))
                .thenCompose(__ -> amqpAdmin().queueUnBindExchange(namespaceName, exchange, queue, params.getProperties_key()))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to update queue {} {} in vhost {}", queue, tenant, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }


    @DELETE
    @Path("/{vhost}/{queue}")
    public void deleteQueue(@Suspended final AsyncResponse response,
                            @PathParam("vhost") String vhost,
                            @PathParam("queue") String queue,
                            QueueDeleteParams params,
                            @QueryParam("authoritative") @DefaultValue("false") boolean authoritative) {
        NamespaceName namespaceName = getNamespaceName(vhost);
        TopicName topicName = TopicName.get(TopicDomain.persistent.toString(),
                namespaceName, PersistentQueue.TOPIC_PREFIX + queue);
        validateTopicOwnershipAsync(topicName, authoritative)
                .thenCompose(__ -> deleteQueueAsync(namespaceName, queue, params.isIfUnused(), params.isIfEmpty()))
                .thenAccept(__ -> {
                    log.info("Success delete queue {} in vhost {}, if-unused is {}, if-empty is {}",
                            queue, vhost, params.isIfUnused(), params.isIfEmpty());
                    response.resume(Response.noContent().build());
                })
                .exceptionally(t -> {
                    if (!isRedirectException(t)) {
                        log.error("Failed to delete queue {} in vhost {}", queue, vhost, t);
                    }
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

}
